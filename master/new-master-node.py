import uuid
import json
import time
import logging
from enum import Enum
from typing import List, Optional
import pika
import boto3
import threading
from threading import Thread, Lock
from functools import wraps

# Logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logging.getLogger("pika").setLevel(logging.WARNING)

# Constants
RABBITMQ_HOST = "172.31.81.152"
RABBITMQ_PORT = 5672
RABBITMQ_CREDS = pika.PlainCredentials("admin", "admin")
HEARTBEAT_INTERVAL = 300  # 5 minutes
CONNECTION_TIMEOUT = 300  # 5 minutes


class CrawlerStatus(Enum):
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
    PROCESSING = "PROCESSING"
    DONE = "DONE"
    ERROR = "ERROR"


# Thread-safe connection manager
class RabbitMQManager:
    _connection_lock = Lock()

    @classmethod
    def get_channel(cls):
        """Get a new channel with thread-safe connection"""
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
            channel = connection.channel()
            channel.exchange_declare(
                exchange="crawler_exchange", exchange_type="direct", durable=False
            )
            channel.queue_declare(queue="crawler_registry", durable=False)
            channel.queue_declare(queue="crawler_updates", durable=False)
            return channel, connection

    @classmethod
    def publish_message(cls, exchange, routing_key, body, max_retries=3):
        """Thread-safe message publishing with retries"""
        for attempt in range(max_retries):
            try:
                channel, connection = cls.get_channel()
                channel.basic_publish(
                    exchange=exchange,
                    routing_key=routing_key,
                    body=body,
                    properties=pika.BasicProperties(
                        delivery_mode=2,
                    ),
                )
                connection.close()
                return True
            except Exception as e:
                logging.error(f"Publish failed (attempt {attempt+1}): {str(e)}")
                time.sleep(2**attempt)
        return False


class Task:
    def __init__(self, seeds: List[str]):
        self.id = str(uuid.uuid4())
        self.seeds = seeds
        self.status = TaskStatus.PENDING
        self.subtasks: List[Subtask] = []

    def create_subtasks(self):
        self.subtasks = [Subtask(parent_task_id=self.id, url=url) for url in self.seeds]
        self.status = TaskStatus.IN_PROGRESS
        logging.info(f"Created {len(self.subtasks)} subtasks for Task {self.id}")

    def check_completion(self):
        if all(st.status == SubtaskStatus.DONE for st in self.subtasks):
            self.status = TaskStatus.COMPLETED
            logging.info(f"Task {self.id} is complete.")


class Subtask:
    def __init__(self, parent_task_id: str, url: str, depth: int = 0):
        self.id = str(uuid.uuid4())
        self.parent_task_id = parent_task_id
        self.url = url
        self.assigned_crawler: Optional[str] = None
        self.status = SubtaskStatus.QUEUED
        self.depth = depth

    def assign_to_crawler(self, crawler_ip: str):
        self.assigned_crawler = crawler_ip
        self.status = SubtaskStatus.ASSIGNED

    def mark_processing(self):
        self.status = SubtaskStatus.PROCESSING

    def mark_done(self):
        self.status = SubtaskStatus.DONE

    def mark_error(self):
        self.status = SubtaskStatus.ERROR


class Crawler:
    def __init__(self, ip: str):
        self.ip = ip
        self.status = CrawlerStatus.IDLE
        self.assigned_subtasks: List[str] = []
        self.last_ping = time.time()

    def assign_task(self, subtask_id: str):
        self.assigned_subtasks.append(subtask_id)
        self.status = CrawlerStatus.RUNNING

    def mark_idle(self):
        self.status = CrawlerStatus.IDLE
        self.assigned_subtasks = []

    def mark_failed(self):
        self.status = CrawlerStatus.FAILED


# Global state
task_map = {}
subtask_map = {}
crawler_map = {}
sqs = boto3.client("sqs", region_name="us-east-1")
queue_url = "https://sqs.us-east-1.amazonaws.com/022499012946/first-sqs-queue.fifo"


def handle_registration(ch, method, properties, body):
    try:
        msg = json.loads(body.decode())
        qn = msg["queue_name"]
        now = msg.get("timestamp", time.time())

        with threading.Lock():
            if qn not in crawler_map:
                crawler_map[qn] = Crawler(qn)
                logging.info(f"Registered crawler {qn}")
            crawler = crawler_map[qn]
            crawler.last_ping = now
            if crawler.status == CrawlerStatus.FAILED:
                crawler.status = CrawlerStatus.IDLE
                logging.info(f"Crawler {qn} recovered from failed state")
    except Exception as e:
        logging.error(f"Error in registration handler: {str(e)}")


def start_registration_listener():
    while True:
        try:
            channel, connection = RabbitMQManager.get_channel()
            channel.basic_consume(
                queue="crawler_registry",
                on_message_callback=handle_registration,
                auto_ack=True,
            )
            logging.info("Registration listener started")
            channel.start_consuming()
        except Exception as e:
            logging.error(f"Registration listener crashed: {str(e)}, restarting in 5s")
            time.sleep(5)
            continue


def get_least_loaded_crawler() -> Optional[Crawler]:
    with threading.Lock():
        valid_crawlers = [
            c for c in crawler_map.values() if c.status != CrawlerStatus.FAILED
        ]
        if not valid_crawlers:
            return None
        return min(valid_crawlers, key=lambda c: len(c.assigned_subtasks))


def health_monitor(timeout=60):
    logging.info("Health monitor started")
    while True:
        now = time.time()
        with threading.Lock():

            for qn, crawler in list(crawler_map.items()):
                if (
                    now - crawler.last_ping > timeout
                    and crawler.status != CrawlerStatus.FAILED
                ):
                    logging.info(
                        f"{qn} timed out, reassigning {len(crawler.assigned_subtasks)} subtasks"
                    )
                    crawler.status = CrawlerStatus.FAILED

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
                                success = RabbitMQManager.publish_message(
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
                subtask.mark_processing()
            elif status == "DONE":
                subtask.mark_done()
                if subtask_id in crawler.assigned_subtasks:
                    crawler.assigned_subtasks.remove(subtask_id)
            elif status == "ERROR":
                subtask.mark_error()
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

                    success = RabbitMQManager.publish_message(
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
            channel, connection = RabbitMQManager.get_channel()
            channel.basic_consume(
                queue="crawler_updates",
                on_message_callback=handle_crawler_updates,
                auto_ack=True,
            )
            logging.info("Crawler update urls started")
            channel.start_consuming()
        except Exception as e:
            logging.error(f"urls listener crashed: {str(e)}, restarting in 5s")
            time.sleep(5)
            continue


def start_crawler_urls_listener():
    while True:
        try:
            channel, connection = RabbitMQManager.get_channel()
            channel.basic_consume(
                queue="crawler_urls",
                on_message_callback=handle_crawler_new_urls,
                auto_ack=True,
            )
            logging.info("Crawler update listener started")
            channel.start_consuming()
        except Exception as e:
            logging.error(f"Update listener crashed: {str(e)}, restarting in 5s")
            time.sleep(5)
            continue


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

                                    success = RabbitMQManager.publish_message(
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


if __name__ == "__main__":
    Thread(target=start_registration_listener, daemon=True).start()
    Thread(target=health_monitor, daemon=True).start()
    Thread(target=start_crawler_update_listener, daemon=True).start()
    Thread(target=start_crawler_urls_listener, daemon=True).start()
    master_process()
