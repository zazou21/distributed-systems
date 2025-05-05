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

mongo_client = MongoClient("mongodb://172.31.82.224:27017")
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
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class SubtaskStatus(Enum):
    QUEUED = "QUEUED"
    ASSIGNED = "ASSIGNED"
    CRAWLER_PROCESSING = "CRAWLER_PROCESSING"
    DONE_CRAWLING = "DONE_CRAWLING"
    ERROR_CRAWLING = "ERROR_CRAWLING"
    INDEXER_PROCESSING = "INDEXER_PROCESSING"
    ERROR_INDEXING = "ERROR_INDEXING"
    DONE = "DONE"


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
sqs = boto3.client("sqs", region_name="us-east-1")
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
            db_channel.queue_declare(queue="db_requests", durable=False)
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


#######################################################          TASK STRUCTURE             #################################################


class Subtask:
    def __init__(self, parent_task_id: str, url: str, depth: int = 0):
        self.id = str(uuid.uuid4())
        self.parent_task_id = parent_task_id
        self.url = url
        self.assigned_crawler: Optional[str] = None
        self.status = SubtaskStatus.QUEUED
        self.depth = depth
        self.subtasks: List[Subtask] = []
        self.assigned_indexer: Optional[str] = None
        self.crawling_time = None
        self.indexing_time = None

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
    def __init__(self, seeds: List[str]):
        self.id = str(uuid.uuid4())
        self.seeds = seeds
        self.status = TaskStatus.PENDING
        self.subtasks: List[Subtask] = []
        self.completion_time = None
        self.start_time = time.time()

    def create_subtasks(self):
        self.subtasks = [Subtask(parent_task_id=self.id, url=url) for url in self.seeds]
        self.status = TaskStatus.IN_PROGRESS
        logging.info(f"Created {len(self.subtasks)} subtasks for Task {self.id}")

    def all_subtasks_done(self, subtasks):
        for st in subtasks:
            if st.status != SubtaskStatus.DONE:
                return False
            if st.subtasks:
                if not self.all_subtasks_done(st.subtasks):
                    return False
        return True

    def check_completion(self):
        if self.all_subtasks_done(self.subtasks):
            self.status = TaskStatus.COMPLETED
            self.completion_time = time.time() - self.start_time

            with task_map_lock:
                task_map.pop(self.id, None)

            with completed_tasks_lock:
                completed_tasks[self.id] = self

            logging.info(f"Task {self.id} is complete.")


def monitor_completion():
    while True:
        for task in list(task_map.values()):
            if task.status == TaskStatus.IN_PROGRESS:
                task.check_completion()
        time.sleep(2)


#################################################          SLAVES             #################################################


class Crawler:
    def __init__(self, ip: str):
        self.ip = ip
        self.status = NodeStatus.IDLE
        self.assigned_subtasks: List[str] = []
        self.last_ping = time.time()

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

    def mark_idle(self):
        self.status = NodeStatus.IDLE

    def mark_failed(self):
        self.status = NodeStatus.FAILED


##########################################################          PROMETHEUS METRICS             #################################################

##################################################          HANDLERS             #################################################


################################################## SLAVE REGISTRATION HANDLER #################################################


def handle_crawler_registration(ch, method, properties, body):
    try:
        msg = json.loads(body.decode())
        qn = msg["queue_name"]
        now = msg.get("timestamp", time.time())

        with crawler_map_lock():
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

        with indexer_map_lock():
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
    with threading.Lock():
        valid_crawlers = [
            c for c in crawler_map.values() if c.status != NodeStatus.FAILED
        ]
        if not valid_crawlers:
            return None
        return min(valid_crawlers, key=lambda c: len(c.assigned_subtasks))


def get_least_loaded_indexer() -> Optional[Indexer]:
    with threading.Lock():
        valid_indexers = [
            i for i in indexer_map.values() if i.status != NodeStatus.FAILED
        ]
        if not valid_indexers:
            return None
        return min(valid_indexers, key=lambda i: len(i.assigned_subtasks))


##################################################         HEALTH MONITORS             #################################################


def crawler_health_monitor(timeout=60):
    logging.info("Health monitor started")
    while True:
        now = time.time()
        with threading.Lock():

            for qn, crawler in list(crawler_map.items()):
                if (
                    now - crawler.last_ping > timeout
                    and crawler.status != NodeStatus.FAILED
                ):
                    logging.info(
                        f"{qn} timed out, reassigning {len(crawler.assigned_subtasks)} subtasks"
                    )
                    crawler.status = NodeStatus.FAILED

                    if crawler.assigned_subtasks:

                        for st_id in crawler.assigned_subtasks[:]:  # Iterate over copy
                            sub = subtask_map.get(st_id)
                            if not sub:
                                continue

                            sub.status = SubtaskStatus.QUEUED
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
                                    logging.info(
                                        f"Reassigned {sub.id} to {new_crawler.ip}"
                                    )
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
                        subtask.status = SubtaskStatus.QUEUED
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

        logging.info(f"Update from {crawler_ip}: Subtask {subtask_id} -> {status}")

        with threading.Lock():
            subtask = subtask_map.get(subtask_id)
            if not subtask:
                logging.warning(f"Unknown subtask {subtask_id}")
                return

            crawler = crawler_map.get(crawler_ip)
            if not crawler:
                logging.warning(f"Unknown crawler {crawler_ip}")
                return

            if status == "PROCESSING":
                subtask.mark_crawler_processing()
            elif status == "DONE":
                subtask.mark_done_crawling()
                if subtask_id in crawler.assigned_subtasks:
                    crawler.assigned_subtasks.remove(subtask_id)
            elif status == "ERROR":
                subtask.mark_crawler_error()
                if subtask_id in crawler.assigned_subtasks:
                    crawler.assigned_subtasks.remove(subtask_id)
                crawler.mark_idle()
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
        depth = int(message["depth"])

        if depth >= 3:
            logging.info(f"Depth limit reached for {parent_subtask_id}")
            return

        logging.info(f"New URLs from {crawler_ip}: {len(urls)} URLs")

        with threading.Lock():
            parent_subtask = subtask_map.get(parent_subtask_id)
            if not parent_subtask:
                logging.warning(f"Unknown parent subtask {parent_subtask_id}")
                return

            for url in urls:
                parent_subtask.subtasks.append(url)
                new_subtask = Subtask(
                    parent_task_id=parent_subtask.parent_task_id,
                    url=url,
                    depth=depth + 1,
                )
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
            elif status == "ERROR":
                subtask.mark_error_indexing()

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

                        logging.info(f"New task: {len(seed_urls)} seeds, depth={depth}")
                        task = Task(seed_urls)
                        with threading.Lock():
                            task_map[task.id] = task

                        task.create_subtasks()

                        with threading.Lock():
                            for subtask in task.subtasks:
                                subtask_map[subtask.id] = subtask
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

                        sqs.delete_message(
                            QueueUrl=queue_url, ReceiptHandle=message["ReceiptHandle"]
                        )
                    except Exception as e:
                        logging.error(f"Error processing SQS message: {str(e)}")

            time.sleep(1)
        except Exception as e:
            logging.error(f"Master process error: {str(e)}, continuing")
            time.sleep(5)


##########################################################         MAIN             #################################################


if __name__ == "__main__":
    Thread(target=start_crawler_registration_listener, daemon=True).start()
    Thread(target=start_indexer_registration_listener, daemon=True).start()
    Thread(target=crawler_health_monitor, daemon=True).start()
    Thread(target=indexer_health_monitor, daemon=True).start()
    Thread(target=start_crawler_update_listener, daemon=True).start()
    Thread(target=start_crawler_urls_listener, daemon=True).start()
    Thread(target=start_indexer_update_listener, daemon=True).start()
    Thread(target=monitor_completion, daemon=True).start()

    master_process()
