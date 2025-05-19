import uuid
import json
import time
import logging
from enum import Enum
from typing import List, Optional
import pika
import boto3
import threading
import prometheus_client
from prometheus_client import start_http_server, Gauge, Counter, Histogram
from threading import Thread, Lock
from functools import wraps
from pymongo import MongoClient


# Logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logging.getLogger("pika").setLevel(logging.WARNING)

###########################################################         CONFIG             #################################################

# Constants
RABBITMQ_HOST = "172.31.81.152"
RABBITMQ_PORT = 5672
RABBITMQ_CREDS = pika.PlainCredentials("admin", "admin")
HEARTBEAT_INTERVAL = 300  # 5 minutes
CONNECTION_TIMEOUT = 300  # 5 minutes

mongo_client = MongoClient("mongodb://mongo1:27017,mongo2:27018/?replicaSet=rs0")

db = mongo_client["distributedDB"]
subtask_collection = db["subtasks"]
task_collection = db["tasks"]
crawler_collection = db["crawlers"]
indexer_collection = db["indexers"]
####################################################         ENUMS             #################################################


class NodeStatus(Enum):
    IDLE = "IDLE"
    RUNNING = "RUNNING"
    FAILED = "FAILED"


class TaskStatus(Enum):
    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    PARTIALLY_COMPLETED = "PARTIALLY_COMPLETED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class SubtaskStatus(Enum):
    QUEUED_FOR_CRAWLING = "QUEUED_FOR_CRAWLING"
    ASSIGNED = "ASSIGNED"
    CRAWLER_PROCESSING = "CRAWLER_PROCESSING"
    DONE_CRAWLING = "DONE_CRAWLING"
    ERROR_CRAWLING = "ERROR_CRAWLING"
    INDEXER_PROCESSING = "INDEXER_PROCESSING"
    ERROR_INDEXING = "ERROR_INDEXING"
    DONE = "DONE"


##########################################################          PROMETHEUS METRICS             #################################################
# Counts
TASK_SUCCESS_COUNT = Counter(
    "task_success_count", "Number of successfully completed subtasks"
)

TASK_ERROR_COUNT = Counter("task_error_count", "Number of subtasks that ended in error")

NODE_FAILURE_COUNT = Counter(
    "node_failure_count", "Number of nodes that failed to respond"
)

# Timing
CRAWLING_TIME = Histogram(
    "crawling_duration_seconds",
    "Time taken to crawl a URL",
    buckets=[0.1, 0.5, 1, 2, 5, 10, 30, 60],
)

INDEXING_TIME = Histogram(
    "indexing_duration_seconds",
    "Time taken to index a page",
    buckets=[0.1, 0.5, 1, 2, 5, 10],
)

TOTAL_TASK_TIME = Histogram(
    "total_task_duration_seconds",
    "Total time taken to complete a full task",
    buckets=[1, 5, 10, 30, 60, 120, 300],
)

CRAWLING_END2END_LATENCY = Histogram(
    "crawling_turnaround_seconds",
    "time taken for a subtask to finish crawling including queue communication ",
)

INDEXING_END2END_LATENCY = Histogram(
    "indexing_turnaround_seconds",
    "time taken for a subtask to finish indexing including queue communication",
)


# Real-time tracking
ACTIVE_CRAWLING_SUBTASKS = Gauge(
    "active_crawling_subtasks", "Current number of subtasks being crawled"
)

ACTIVE_INDEXING_SUBTASKS = Gauge(
    "active_indexing_subtasks", "Current number of subtasks being indexed"
)

PENDING_SUBTASKS = Gauge("pending_subtasks", "Current number of subtasks in queue")


######################################################         GLOBAL VARIABLES             #################################################
task_map = {}
completed_tasks = {}
subtask_map = {}
crawler_map = {}
indexer_map = {}

task_map_lock = threading.Lock()
completed_tasks_lock = threading.Lock()
crawler_map_lock = threading.Lock()
indexer_map_lock = threading.Lock()
subtask_map_lock = threading.Lock()
sqs = boto3.client("sqs", region_name="us-east-1")
master_server_queue_url = (
    "https://sqs.us-east-1.amazonaws.com/022499012946/queue-master-server.fifo"
)
queue_url = "https://sqs.us-east-1.amazonaws.com/022499012946/first-sqs-queue.fifo"


###################################################         RABBITMQ MANAGER             #################################################
class RabbitMQManager:
    _connection_lock = Lock()

    @classmethod
    def get_crawler_channel(cls):
        """Get a new crawler_channel with thread-safe connection"""
        params = pika.ConnectionParameters(
            host=RABBITMQ_HOST,
            port=RABBITMQ_PORT,
            credentials=RABBITMQ_CREDS,
            heartbeat=HEARTBEAT_INTERVAL,
            blocked_connection_timeout=CONNECTION_TIMEOUT,
            socket_timeout=10,
        )

        with cls._connection_lock:
            connection = pika.BlockingConnection(params)
            crawler_channel = connection.channel()
            crawler_channel.exchange_declare(
                exchange="crawler_exchange", exchange_type="direct", durable=False
            )
            crawler_channel.queue_declare(queue="crawler_registry", durable=False)
            crawler_channel.queue_declare(queue="crawler_updates", durable=False)
            crawler_channel.queue_declare(queue="crawler_urls", durable=False)

            return crawler_channel, connection

    @classmethod
    def get_indexer_channel(cls):
        """Get a new indexer_channel with thread-safe connection"""
        params = pika.ConnectionParameters(
            host=RABBITMQ_HOST,
            port=RABBITMQ_PORT,
            credentials=RABBITMQ_CREDS,
            heartbeat=HEARTBEAT_INTERVAL,
            blocked_connection_timeout=CONNECTION_TIMEOUT,
            socket_timeout=10,
        )

        with cls._connection_lock:
            connection = pika.BlockingConnection(params)
            indexer_channel = connection.channel()
            indexer_channel.exchange_declare(
                exchange="indexer_exchange", exchange_type="direct", durable=False
            )
            indexer_channel.queue_declare(queue="indexer_updates", durable=False)
            indexer_channel.queue_declare(queue="indexer_registry", durable=False)

            return indexer_channel, connection

    @classmethod
    def get_db_channel(cls):
        """Get a new db_channel with thread-safe connection"""
        params = pika.ConnectionParameters(
            host=RABBITMQ_HOST,
            port=RABBITMQ_PORT,
            credentials=RABBITMQ_CREDS,
            heartbeat=HEARTBEAT_INTERVAL,
            blocked_connection_timeout=CONNECTION_TIMEOUT,
            socket_timeout=10,
        )

        with cls._connection_lock:
            connection = pika.BlockingConnection(params)
            db_channel = connection.channel()
            db_channel.exchange_declare(
                exchange="db_exchange", exchange_type="direct", durable=False
            )

            db_channel.queue_declare(queue="crawlers", durable=False)
            db_channel.queue_declare(queue="indexers", durable=False)
            db_channel.queue_declare(queue="task_registry", durable=False)
            db_channel.queue_declare(queue="subtask_registry", durable=False)

            db_channel.queue_bind(
                exchange="db_exchange",
                queue="task_registry",
                routing_key="task_registry",
            )
            db_channel.queue_bind(
                exchange="db_exchange",
                queue="subtask_registry",
                routing_key="subtask_registry",
            )
            db_channel.queue_bind(
                exchange="db_exchange", queue="crawlers", routing_key="crawlers"
            )
            db_channel.queue_bind(
                exchange="db_exchange", queue="indexers", routing_key="indexers"
            )

            return db_channel, connection

    @classmethod
    def crawler_publish_message(cls, exchange, routing_key, body, max_retries=3):
        """Thread-safe message publishing with retries"""
        for attempt in range(max_retries):
            try:
                crawler_channel, connection = cls.get_crawler_channel()
                try:
                    crawler_channel.basic_publish(
                        exchange=exchange,
                        routing_key=routing_key,
                        body=body,
                        properties=pika.BasicProperties(
                            delivery_mode=2,
                        ),
                    )
                    connection.close()
                    return True
                except pika.exceptions.AMQPError as e:
                    logging.error(f"AMQP error: {str(e)}")
                    connection.close()
                    return False
            except Exception as e:
                logging.error(f"Publish failed (attempt {attempt+1}): {str(e)}")
                time.sleep(2**attempt)
        logging.error(f"Failed to publish message after {max_retries} attempts")

        return False

    @classmethod
    def indexer_publish_message(cls, exchange, routing_key, body, max_retries=3):
        """Thread-safe message publishing with retries"""
        for attempt in range(max_retries):
            try:
                indexer_channel, connection = cls.get_indexer_channel()
                try:
                    indexer_channel.basic_publish(
                        exchange=exchange,
                        routing_key=routing_key,
                        body=body,
                        properties=pika.BasicProperties(
                            delivery_mode=2,
                        ),
                    )
                    connection.close()
                    return True
                except pika.exceptions.AMQPError as e:
                    logging.error(f"AMQP error: {str(e)}")
                    connection.close()
                    return False

            except Exception as e:
                logging.error(f"Publish failed (attempt {attempt+1}): {str(e)}")
                time.sleep(2**attempt)
        return False

    @classmethod
    def db_publish_message(cls, exchange, routing_key, body, max_retries=3):
        """Thread-safe message publishing with retries"""
        for attempt in range(max_retries):
            try:
                db_channel, connection = cls.get_db_channel()
                try:
                    db_channel.basic_publish(
                        exchange="",
                        routing_key=routing_key,
                        body=body,
                        properties=pika.BasicProperties(
                            delivery_mode=2,
                        ),
                    )
                    connection.close()
                    return True
                except pika.exceptions.AMQPError as e:
                    logging.error(f"AMQP error: {str(e)}")
                    connection.close()
                    return False

            except Exception as e:
                logging.error(f"Publish failed (attempt {attempt+1}): {str(e)}")
                time.sleep(2**attempt)
        return False


#######################################################          TASK STRUCTURE             #################################################


class Subtask:
    def __init__(self, parent_task_id: str, url: str, depth: int = 0):
        self.id = str(uuid.uuid4())
        self.parent_task_id = parent_task_id
        self.url = url
        self.assigned_crawler: Optional[str] = None
        self.status = SubtaskStatus.QUEUED_FOR_CRAWLING
        self.depth = depth
        self.subtasks: List[str] = []
        self.assigned_indexer: Optional[str] = None
        self.creation_time = time.time()
        self.crawling_time = None
        self.end_crawling_time = None
        self.indexing_time = None
        self.end_time = None

    @classmethod
    def from_dict(cls, data):

        subtask = cls(
            parent_task_id=data.get("parent_task_id", ""),
            url=data.get("url", ""),
            depth=data.get("depth", 0),
        )
        subtask.id = data.get("id", subtask.id)
        subtask.assigned_crawler = data.get("assigned_crawler")
        subtask.status = data.get("status", subtask.status)
        subtask.subtasks = data.get("subtasks", [])
        subtask.assigned_indexer = data.get("assigned_indexer")
        subtask.creation_time = data.get("creation_time", subtask.creation_time)
        subtask.crawling_time = data.get("crawling_time")
        subtask.end_crawling_time = data.get("end_crawling_time")
        subtask.indexing_time = data.get("indexing_time")
        subtask.end_time = data.get("end_time")

        return subtask

    def assign_to_crawler(self, crawler_ip: str):
        self.assigned_crawler = crawler_ip
        self.status = SubtaskStatus.ASSIGNED

    def mark_crawler_processing(self):
        self.status = SubtaskStatus.CRAWLER_PROCESSING

    def mark_done_crawling(self):
        self.status = SubtaskStatus.DONE_CRAWLING

    def mark_error_crawling(self):
        self.status = SubtaskStatus.ERROR_CRAWLING

    def mark_indexer_processing(self):
        self.status = SubtaskStatus.INDEXER_PROCESSING

    def mark_done_indexing(self):
        self.status = SubtaskStatus.DONE


class Task:
    def __init__(self, id, seeds: List[str]):
        self.id = id
        self.seeds = seeds
        self.status = TaskStatus.PENDING
        self.subtasks: List[str] = []
        self.start_time = time.time()
        self.completion_time = None
        self.creation_time = time.time()
        self.sent_message = False

    @classmethod
    def from_dict(cls, data):
        task = cls(seeds=data["seeds"], id=data["id"])
        task.id = data.get("id", task.id)
        task.status = TaskStatus[data.get("status", task.status.name)]
        task.subtasks = data.get("subtasks", [])
        task.start_time = data.get("start_time", task.start_time)
        task.completion_time = data.get("completion_time", None)
        task.creation_time = data.get("creation_time", task.creation_time)
        task.sent_message = (
            data.get("sent_message", task.sent_message)
            if data.get("sent_message")
            else False
        )
        return task

    def create_subtasks(self):
        for url in self.seeds:
            subtask = Subtask(parent_task_id=self.id, url=url)
            self.subtasks.append(subtask.id)
            subtask_map[subtask.id] = subtask
            subtask_data = subtask.__dict__
            subtask_data = enum_to_str(subtask_data)
            success = RabbitMQManager.db_publish_message(
                exchange="",
                routing_key="subtask_registry",
                body=f"{json.dumps(subtask_data)}",
            )
            if success:
                logging.info(f"Published subtask {subtask.id} to DB")

            else:
                logging.error(f"Failed to publish subtask {subtask.id} to DB")

        self.status = TaskStatus.IN_PROGRESS
        logging.info(f"Created {len(self.subtasks)} subtasks for Task {self.id}")

    def subtask_completion_ratio(self, subtasks_ids, visited=None):
        if visited is None:
            visited = set()

        done = 0
        total = 0

        # Use a local copy to avoid race conditions
        with subtask_map_lock:
            subtasks_to_process = [
                subtask_map.get(st_id) for st_id in subtasks_ids if st_id not in visited
            ]
            # Filter out None values in case subtasks were deleted
            subtasks_to_process = [st for st in subtasks_to_process if st]

        for st in subtasks_to_process:
            if st.id in visited:
                continue

            visited.add(st.id)
            total += 1

            if st.status in (
                SubtaskStatus.DONE,
                SubtaskStatus.ERROR_CRAWLING,
                SubtaskStatus.ERROR_INDEXING,
            ):
                done += 1

            if st.subtasks:
                sub_done, sub_total = self.subtask_completion_ratio(
                    st.subtasks, visited
                )
                done += sub_done
                total += sub_total

        return done, total

    def check_completion(self):
        done, total = self.subtask_completion_ratio(self.subtasks)
        if total == 0:
            return

        ratio = done / total

        if ratio >= 0.4:
            self.status = TaskStatus.PARTIALLY_COMPLETED
            try:
                if not self.sent_message:
                    response = sqs.send_message(
                        QueueUrl=master_server_queue_url,
                        MessageBody=json.dumps(
                            {"task_id": self.id, "status": "COMPLETED"}
                        ),
                        MessageGroupId="master-server-group",
                        MessageDeduplicationId=str(uuid.uuid4()),
                    )
                    logging.info(
                        f"Sent task completion to server queue | Task ID: {self.id} | MessageId: {response.get('MessageId')}"
                    )
                    self.sent_message = True
            except Exception as e:
                logging.error(
                    f"Failed to notify server of task completion | Task ID: {self.id} | Error: {str(e)}"
                )

        if ratio >= 0.99:
            self.status = TaskStatus.COMPLETED
            self.completion_time = time.time() - self.start_time
            task_map.pop(self.id, None)
            completed_tasks[self.id] = self
            TOTAL_TASK_TIME.observe(self.completion_time)
            logging.info(f"Task {self.id} is complete.")

            # Send task completion notification to server


def monitor_completion():
    while True:
        logging.info("Checking for completed tasks...")
        with task_map_lock:
            for task in list(task_map.values()):
                task.check_completion()
        time.sleep(5)


#################################################          SLAVES             #################################################


class Crawler:
    def __init__(self, ip: str):
        self.ip = ip
        self.status = NodeStatus.IDLE
        self.assigned_subtasks: List[str] = []
        self.last_ping = time.time()

    @classmethod
    def from_dict(cls, data):
        crawler = cls(ip=data["ip"])
        crawler.status = data.get("status", crawler.status)
        crawler.assigned_subtasks = data.get("assigned_subtasks", [])
        crawler.last_ping = data.get("last_ping", crawler.last_ping)
        return crawler

    def assign_task(self, subtask_id: str):
        self.assigned_subtasks.append(subtask_id)
        self.status = NodeStatus.RUNNING

    def mark_idle(self):
        self.status = NodeStatus.IDLE
        self.assigned_subtasks = []

    def mark_failed(self):
        self.status = NodeStatus.FAILED


class Indexer:
    def __init__(self, ip: str):
        self.ip = ip
        self.status = NodeStatus.IDLE
        self.assigned_subtasks: List[str] = []
        self.last_ping = time.time()

    @classmethod
    def from_dict(cls, data):
        indexer = cls(ip=data["ip"])
        indexer.status = data.get("status", indexer.status)
        indexer.assigned_subtasks = data.get("assigned_subtasks", [])
        indexer.last_ping = data.get("last_ping", indexer.last_ping)
        return indexer

    def assign_task(self, subtask_id: str):
        self.assigned_subtasks.append(subtask_id)
        self.status = NodeStatus.RUNNING

    def mark_idle(self):
        self.status = NodeStatus.IDLE

    def mark_failed(self):
        self.status = NodeStatus.FAILED


##################################################          HANDLERS             #################################################


################################################## SLAVE REGISTRATION HANDLER #################################################


def handle_crawler_registration(ch, method, properties, body):
    try:
        msg = json.loads(body.decode())
        qn = msg["queue_name"]
        now = msg.get("timestamp", time.time())

        with crawler_map_lock:
            if qn not in crawler_map:
                crawler_map[qn] = Crawler(qn)
                logging.info(f"Registered crawler {qn}")
            crawler = crawler_map[qn]
            crawler.last_ping = now
            if crawler.status == NodeStatus.FAILED:
                crawler.status = NodeStatus.IDLE
                logging.info(f"Crawler {qn} recovered from failed state")
    except Exception as e:
        logging.error(f"Error in registration handler: {str(e)}")


def start_crawler_registration_listener():
    while True:
        try:
            crawler_channel, connection = RabbitMQManager.get_crawler_channel()
            crawler_channel.basic_consume(
                queue="crawler_registry",
                on_message_callback=handle_crawler_registration,
                auto_ack=True,
            )
            logging.info("Registration listener started")
            crawler_channel.start_consuming()
        except Exception as e:
            logging.error(f"Registration listener crashed: {str(e)}, restarting in 5s")
            time.sleep(5)
            continue


def handle_indexer_registration(ch, method, properties, body):
    try:
        msg = json.loads(body.decode())
        qn = msg["queue_name"]
        now = msg.get("timestamp", time.time())

        with indexer_map_lock:
            if qn not in indexer_map:
                indexer_map[qn] = Indexer(qn)
                logging.info(f"Registered indexer {qn}")
            indexer = indexer_map[qn]
            indexer.last_ping = now
            if indexer.status == NodeStatus.FAILED:
                indexer.status = NodeStatus.IDLE
                logging.info(f"Indexer {qn} recovered from failed state")
    except Exception as e:
        logging.error(f"Error in registration handler: {str(e)}")


def start_indexer_registration_listener():
    while True:
        try:
            indexer_channel, connection = RabbitMQManager.get_indexer_channel()
            indexer_channel.basic_consume(
                queue="indexer_registry",
                on_message_callback=handle_indexer_registration,
                auto_ack=True,
            )
            logging.info("Indexer registration listener started")
            indexer_channel.start_consuming()
        except Exception as e:
            logging.error(
                f"Indexer registration listener crashed: {str(e)}, restarting in 5s"
            )
            time.sleep(5)
            continue


###################################################         UTILITIES           ############################################################


def get_least_loaded_crawler() -> Optional[Crawler]:
    with crawler_map_lock:
        valid_crawlers = [
            c for c in crawler_map.values() if c.status != NodeStatus.FAILED
        ]
        if not valid_crawlers:
            return None
        return min(valid_crawlers, key=lambda c: len(c.assigned_subtasks))


def get_least_loaded_indexer() -> Optional[Indexer]:
    with indexer_map_lock:
        valid_indexers = [
            i for i in indexer_map.values() if i.status != NodeStatus.FAILED
        ]
        if not valid_indexers:
            return None
        return min(valid_indexers, key=lambda i: len(i.assigned_subtasks))


##################################################         HEALTH MONITORS             #################################################
def assign_queued_subtasks():
    while True:
        with subtask_map_lock, crawler_map_lock:
            for subtask in list(subtask_map.values()):
                if subtask.status == SubtaskStatus.QUEUED_FOR_CRAWLING:
                    crawler = get_least_loaded_crawler()
                    if crawler:
                        subtask.assign_to_crawler(crawler.ip)
                        crawler.assign_task(subtask.id)

                        success = RabbitMQManager.crawler_publish_message(
                            exchange="crawler_exchange",
                            routing_key=crawler.ip,
                            body=f"{subtask.id}|{subtask.url}|{crawler.ip}|{subtask.depth}",
                        )
                        if success:
                            logging.info(f"Dispatched {subtask.id} to {crawler.ip}")
                            ACTIVE_CRAWLING_SUBTASKS.inc()
                        else:
                            logging.error(f"Failed to dispatch {subtask.id}")
        time.sleep(10)


def crawler_health_monitor(timeout=60):
    logging.info("Health monitor started")
    while True:
        now = time.time()

        for qn, crawler in list(crawler_map.items()):
            if (
                now - crawler.last_ping > timeout
                and crawler.status != NodeStatus.FAILED
            ):

                logging.info(
                    f"{qn} timed out, reassigning {len(crawler.assigned_subtasks)} subtasks"
                )
                crawler.status = NodeStatus.FAILED
                NODE_FAILURE_COUNT.inc()

                if crawler.assigned_subtasks:

                    for st_id in crawler.assigned_subtasks[:]:  # Iterate over copy
                        sub = subtask_map.get(st_id)
                        if not sub:
                            continue

                        sub.status = SubtaskStatus.QUEUED_FOR_CRAWLING
                        new_crawler = get_least_loaded_crawler()
                        if new_crawler:
                            sub.assign_to_crawler(new_crawler.ip)
                            new_crawler.assign_task(sub.id)
                            success = RabbitMQManager.crawler_publish_message(
                                exchange="crawler_exchange",
                                routing_key=new_crawler.ip,
                                body=f"{sub.id}|{sub.url}|{new_crawler.ip}|{sub.depth}",
                            )
                            if success:
                                logging.info(f"Reassigned {sub.id} to {new_crawler.ip}")
                            else:
                                logging.error(f"Failed to reassign {sub.id}")

                    crawler.assigned_subtasks.clear()
        time.sleep(timeout / 2)


def indexer_health_monitor():
    logging.info("Indexer health monitor started")
    while True:
        now = time.time()
        for qn, indexer in list(indexer_map.items()):
            if now - indexer.last_ping > 60 and indexer.status != NodeStatus.FAILED:

                logging.info(f"{qn} timed out, marking as failed")
                indexer.status = NodeStatus.FAILED
                NODE_FAILURE_COUNT.inc()
                for st_id in indexer.assigned_subtasks[:]:  # Iterate over copy
                    try:
                        subtask_info = subtask_collection.find_one({"id": st_id})
                    except Exception as e:
                        logging.error(
                            f"Error fetching subtask {st_id} from mongo: {str(e)}"
                        )
                        continue
                    if not subtask_info:
                        logging.warning(f"Subtask {st_id} not found in DB")
                        continue
                    indexer.assigned_subtasks.remove(st_id)
                    new_indexer = get_least_loaded_indexer()
                    if new_indexer:
                        subtask = subtask_map.get(st_id)
                        subtask.status = SubtaskStatus.QUEUED_FOR_INDEXING
                        subtask.assigned_indexer = new_indexer.ip
                        new_indexer.assigned_subtasks.append(st_id)
                        success = RabbitMQManager.crawler_publish_message(
                            exchange="indexer_exchange",
                            routing_key=new_indexer.ip,
                            body=f"{st_id}|{subtask_info['url']}|{subtask_info['extracted_content']}|{new_indexer.ip}",
                        )
                        if not success:
                            logging.error(
                                f"Failed to reassign {st_id} to {new_indexer.ip}"
                            )
                        else:
                            logging.info(f"Reassigned {st_id} to {new_indexer.ip}")
                indexer.assigned_subtasks.clear()

        time.sleep(30)


###################################################       SLAVE UPDATES HANDLERS             #################################################


def handle_crawler_updates(ch, method, properties, body):
    try:
        message = json.loads(body.decode())
        subtask_id = message["subtask_id"]
        crawler_ip = message["crawler_ip"]
        status = message["status"]
        data = message.get("data", {})

        logging.info(
            f"Update from crawler {crawler_ip}: Subtask {subtask_id} -> {status}"
        )

        with subtask_map_lock:
            subtask = subtask_map.get(subtask_id)
            if not subtask:
                logging.warning(f"Unknown subtask {subtask_id}")
                return
            if subtask.status == SubtaskStatus.DONE:
                return

            crawler = crawler_map.get(crawler_ip)
            if not crawler:
                logging.warning(f"Unknown crawler {crawler_ip}")
                return

            if status == "PROCESSING":
                subtask.mark_crawler_processing()
            elif status == "DONE":
                subtask.mark_done_crawling()
                crawling_time = data.get("crawling_time", 0)
                subtask.crawling_time = crawling_time
                subtask.end_crawling_time = time.time()
                turnaround = subtask.end_crawling_time - subtask.creation_time
                CRAWLING_TIME.observe(crawling_time)
                CRAWLING_END2END_LATENCY.observe(turnaround)
                ACTIVE_CRAWLING_SUBTASKS.dec()
                ACTIVE_INDEXING_SUBTASKS.inc()

                if subtask_id in crawler.assigned_subtasks:
                    crawler.assigned_subtasks.remove(subtask_id)
            elif status == "ERROR":
                subtask.mark_error_crawling()
                if subtask_id in crawler.assigned_subtasks:
                    crawler.assigned_subtasks.remove(subtask_id)
                crawler.mark_idle()
                subtask.end_crawling_time = time.time()
                turnaround = subtask.end_crawling_time - subtask.creation_time
                CRAWLING_END2END_LATENCY.observe(turnaround)
                ACTIVE_CRAWLING_SUBTASKS.dec()
                TASK_ERROR_COUNT.inc()
    except Exception as e:
        logging.error(f"Error in update handler: {str(e)}")


def handle_crawler_new_urls(ch, method, properties, body):
    try:
        message = json.loads(body.decode())
        parent_subtask_id = message.get("parent_subtask_id")
        if parent_subtask_id is None:
            logging.error(f"Malformed message: {message}")
            return
        crawler_ip = message["crawler_ip"]
        urls = message["urls"]

        parent_subtask = subtask_map.get(parent_subtask_id)
        if not parent_subtask:
            logging.warning(f"Unknown parent subtask {parent_subtask_id}")
            return
        depth = parent_subtask.depth + 1

        if depth >= 3:
            logging.info(f"Depth limit reached for {parent_subtask_id}")
            return

        logging.info(f"New URLs from {crawler_ip}: {len(urls)} URLs")

        with subtask_map_lock:

            for url in urls:

                new_subtask = Subtask(
                    parent_task_id=parent_subtask.parent_task_id,
                    url=url,
                    depth=depth,
                )
                parent_subtask.subtasks.append(new_subtask.id)
                subtask_map[new_subtask.id] = new_subtask

                assigned_crawler = get_least_loaded_crawler()
                if assigned_crawler:
                    new_subtask.assign_to_crawler(assigned_crawler.ip)
                    assigned_crawler.assign_task(new_subtask.id)

                    success = RabbitMQManager.crawler_publish_message(
                        exchange="crawler_exchange",
                        routing_key=assigned_crawler.ip,
                        body=f"{new_subtask.id}|{new_subtask.url}|{crawler_ip}|{new_subtask.depth}",
                    )
                    if success:
                        logging.info(
                            f"Dispatched {new_subtask.id} to {assigned_crawler.ip}"
                        )
                        ACTIVE_CRAWLING_SUBTASKS.inc()
                    else:
                        logging.error(f"Failed to dispatch {new_subtask.id}")
    except Exception as e:
        logging.error(f"Error in new URLs handler: {str(e)}")


def start_crawler_update_listener():
    while True:
        try:
            crawler_channel, connection = RabbitMQManager.get_crawler_channel()
            crawler_channel.basic_consume(
                queue="crawler_updates",
                on_message_callback=handle_crawler_updates,
                auto_ack=True,
            )
            logging.info("Crawler update urls started")
            crawler_channel.start_consuming()
        except Exception as e:
            logging.error(f"urls listener crashed: {str(e)}, restarting in 5s")
            time.sleep(5)
            continue


def start_crawler_urls_listener():
    while True:
        try:
            crawler_channel, connection = RabbitMQManager.get_crawler_channel()
            crawler_channel.basic_consume(
                queue="crawler_urls",
                on_message_callback=handle_crawler_new_urls,
                auto_ack=True,
            )
            logging.info("Crawler update listener started")
            crawler_channel.start_consuming()
        except Exception as e:
            logging.error(f"Update listener crashed: {str(e)}, restarting in 5s")
            time.sleep(5)
            continue


def handle_indexer_updates(ch, method, properties, body):
    try:
        message = json.loads(body.decode())
        subtask_id = message["subtask_id"]
        indexer_ip = message["indexer_ip"]
        status = message["status"]
        data = message.get("data", {})
        logging.info(
            f"Update from indexer {indexer_ip}: Subtask {subtask_id} -> {status}"
        )
        with threading.Lock():
            subtask = subtask_map.get(subtask_id)
            if not subtask:
                logging.warning(f"Unknown subtask {subtask_id}")
                return

            if status == "PROCESSING":
                subtask.mark_indexer_processing()
                subtask = subtask_map.get(subtask_id)
                indexer = indexer_map.get(indexer_ip)
                indexer.assigned_subtasks.append(subtask_id)

            elif status == "DONE":
                subtask.mark_done_indexing()
                indexer = indexer_map.get(indexer_ip)
                if indexer and subtask_id in indexer.assigned_subtasks:
                    indexer.assigned_subtasks.remove(subtask_id)
                subtask.indexing_time = data.get("indexing_time", 0)
                subtask.end_time = time.time()
                turnaround = subtask.end_time - subtask.end_crawling_time
                TASK_SUCCESS_COUNT.inc()
                INDEXING_TIME.observe(subtask.indexing_time)
                INDEXING_END2END_LATENCY.observe(turnaround)
                ACTIVE_INDEXING_SUBTASKS.dec()
            elif status == "ERROR":
                subtask.mark_error_indexing()
                ACTIVE_INDEXING_SUBTASKS.dec()
                TASK_ERROR_COUNT.inc()

    except Exception as e:
        logging.error(f"Error in indexer update handler: {str(e)}")


def start_indexer_update_listener():
    while True:
        try:
            crawler_channel, connection = RabbitMQManager.get_crawler_channel()
            crawler_channel.basic_consume(
                queue="indexer_updates",
                on_message_callback=handle_indexer_updates,
                auto_ack=True,
            )
            logging.info("Indexer update listener started")
            crawler_channel.start_consuming()
        except Exception as e:
            logging.error(
                f"Indexer update listener crashed: {str(e)}, restarting in 5s"
            )
            time.sleep(5)
            continue


##############################################              BACKUP                     ################################################


def enum_to_str(data):
    """Converts enum values to their string representations."""
    for key, value in data.items():
        if isinstance(value, Enum):
            data[key] = value.name  # Convert enum to its name (string)
    return data


def periodic_master_backup():
    while True:
        try:
            with task_map_lock:
                for task in list(task_map.values()):
                    task_data = task.__dict__
                    task_data = enum_to_str(task_data)  # Convert enums to strings
                    task_collection.update_one(
                        {"id": task.id}, {"$set": task_data}, upsert=True
                    )
        except Exception as e:
            logging.error(f"Error during task backup: {str(e)}")
        try:
            with subtask_map_lock:
                for subtask in list(subtask_map.values()):
                    subtask_data = subtask.__dict__
                    subtask_data = enum_to_str(subtask_data)  # Convert enums to strings
                    subtask_collection.update_one(
                        {"subtask_id": subtask.id}, {"$set": subtask_data}, upsert=True
                    )
        except Exception as e:
            logging.error(f"Error during subtask backup: {str(e)}")
        try:
            with crawler_map_lock:
                for crawler in list(crawler_map.values()):
                    crawler_data = crawler.__dict__
                    crawler_data = enum_to_str(crawler_data)  # Convert enums to strings
                    crawler_collection.update_one(
                        {"crawler_ip": crawler.ip}, {"$set": crawler_data}, upsert=True
                    )
        except Exception as e:
            logging.error(f"Error during crawler backup: {str(e)}")
        try:
            with indexer_map_lock:
                for indexer in list(indexer_map.values()):
                    indexer_data = indexer.__dict__
                    indexer_data = enum_to_str(indexer_data)  # Convert enums to strings
                    indexer_collection.update_one(
                        {"indexer_ip": indexer.ip}, {"$set": indexer_data}, upsert=True
                    )
            logging.info("Master backup completed")

        except Exception as e:
            logging.error(f"Error during indexer backup: {str(e)}")

        time.sleep(180)


def get_backup_data():
    try:
        # Load tasks
        for task_doc in task_collection.find():
            task_doc.pop("_id", None)
            task = Task.from_dict(task_doc)
            with task_map_lock:
                task_map[task.id] = task

            # Load subtasks
            for subtask_doc in subtask_collection.find():
                try:
                    subtask_doc.pop("_id", None)
                    subtask = Subtask.from_dict(subtask_doc)
                    with subtask_map_lock:
                        subtask_map[subtask.id] = subtask
                except Exception as e:
                    continue

        # Load crawlers
        for crawler_doc in crawler_collection.find():
            crawler_doc.pop("_id", None)
            crawler = Crawler.from_dict(crawler_doc)
            with crawler_map_lock:
                crawler_map[crawler.ip] = crawler

        # Load indexers
        for indexer_doc in indexer_collection.find():
            indexer_doc.pop("_id", None)
            indexer = Indexer.from_dict(indexer_doc)
            with indexer_map_lock:
                indexer_map[indexer.ip] = indexer

        logging.info("Master backup successfully loaded from MongoDB")
    except Exception as e:
        logging.error(f"Error loading backup data: {str(e)}")


######################################################      MASTER PROCESS             #################################################


def master_process():
    logging.info("Master process started")
    Thread(target=start_crawler_update_listener, daemon=True).start()

    while True:
        try:
            response = sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=10,
                AttributeNames=["All"],
            )

            messages = response.get("Messages", [])
            if messages:
                for message in messages:
                    try:
                        body = json.loads(message["Body"])
                        seed_urls = body.get("seeds", [])
                        depth = body.get("depth", 1)
                        id = body.get("task_id")

                        logging.info(f"New task: {len(seed_urls)} seeds, depth={depth}")
                        task = Task(id, seed_urls)

                        task_data = task.__dict__
                        task_data = enum_to_str(task_data)  # Convert enums to strings

                        success = RabbitMQManager.db_publish_message(
                            exchange="",
                            routing_key="task_registry",
                            body=json.dumps(task_data),
                        )
                        if not success:
                            logging.error(f"Failed to publish task {task.id} to DB")
                        elif success:
                            logging.info(f"Published task {task.id} to DB")

                        with task_map_lock:
                            task_map[task.id] = task

                        task.create_subtasks()

                        with threading.Lock():
                            for subtask_id in task.subtasks:
                                subtask = subtask_map.get(subtask_id)
                                crawler = get_least_loaded_crawler()
                                if crawler:
                                    subtask.assign_to_crawler(crawler.ip)
                                    crawler.assign_task(subtask.id)

                                    success = RabbitMQManager.crawler_publish_message(
                                        exchange="crawler_exchange",
                                        routing_key=crawler.ip,
                                        body=f"{subtask.id}|{subtask.url}|{crawler.ip}|{subtask.depth}",
                                    )
                                    if success:
                                        logging.info(
                                            f"Dispatched {subtask.id} to {crawler.ip}"
                                        )
                                    else:
                                        logging.error(
                                            f"Failed to dispatch {subtask.id}"
                                        )
                                else:
                                    logging.info(
                                        f"No available crawlers for subtask {subtask.id}"
                                    )
                                    continue

                        sqs.delete_message(
                            QueueUrl=queue_url, ReceiptHandle=message["ReceiptHandle"]
                        )
                    except Exception as e:
                        logging.error(f"Error processing SQS message: {str(e)}")
                        sqs.delete_message(
                            QueueUrl=queue_url, ReceiptHandle=message["ReceiptHandle"]
                        )

            time.sleep(1)
        except Exception as e:
            logging.error(f"Master process error: {str(e)}, continuing")
            time.sleep(5)


##########################################################         MAIN             #################################################


if __name__ == "__main__":
    get_backup_data()
    start_http_server(8000)
    Thread(target=periodic_master_backup, daemon=True).start()
    Thread(target=start_crawler_registration_listener, daemon=True).start()
    Thread(target=start_indexer_registration_listener, daemon=True).start()
    Thread(target=crawler_health_monitor, daemon=True).start()
    Thread(target=indexer_health_monitor, daemon=True).start()
    Thread(target=start_crawler_update_listener, daemon=True).start()
    Thread(target=start_crawler_urls_listener, daemon=True).start()
    Thread(target=start_indexer_update_listener, daemon=True).start()
    Thread(target=monitor_completion, daemon=True).start()
    # Thread(target=assign_queued_subtasks, daemon=True).start()

    master_process()
