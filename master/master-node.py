# master_node.py

import uuid
import json
import time
import logging
from enum import Enum
from typing import List, Optional

import pika
import boto3
from threading import Thread

# Logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


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


class Task:
    def _init_(self, seeds: List[str]):
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
    def _init_(self, parent_task_id: str, url: str):
        self.id = str(uuid.uuid4())
        self.parent_task_id = parent_task_id
        self.url = url
        self.assigned_crawler: Optional[str] = None
        self.status = SubtaskStatus.QUEUED

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
    def _init_(self, ip: str):
        self.ip = ip
        self.status = "IDLE"
        self.assigned_subtasks: List[str] = []

    def assign_task(self, subtask_id: str):
        self.assigned_subtasks.append(subtask_id)
        self.status = "BUSY"

    def mark_idle(self):
        self.status = "IDLE"


# RabbitMQ setup
connection = pika.BlockingConnection(
    pika.ConnectionParameters(
        "172.31.81.152", 5672, credentials=pika.PlainCredentials("admin", "admin")
    )
)
channel = connection.channel()
channel.exchange_declare(exchange="crawler_exchange", exchange_type="direct")
channel.queue_declare(queue="crawler_updates")

# SQS setup
sqs = boto3.client("sqs", region_name="us-east-1")
queue_url = "https://sqs.us-east-1.amazonaws.com/022499012946/first-sqs-queue.fifo"

task_map = {}
subtask_map = {}
crawler_map = {}


def get_least_loaded_crawler(crawlers: List[Crawler]) -> Optional[Crawler]:
    if not crawlers:
        return None
    return min(crawlers, key=lambda c: len(c.assigned_subtasks))


def handle_crawler_updates(ch, method, properties, body):
    message = json.loads(body.decode())
    subtask_id = message["subtask_id"]
    crawler_ip = message["crawler_ip"]
    status = message["status"]
    data = message.get("data", {})

    logging.info(f"Update from {crawler_ip}: Subtask {subtask_id} -> {status}")

    subtask = subtask_map.get(subtask_id)
    if not subtask:
        logging.warning(f"Unknown subtask {subtask_id}")
        return

    if status == "PROCESSING":
        subtask.mark_processing()
    elif status == "DONE":
        subtask.mark_done()
        crawler_map[crawler_ip].assigned_subtasks.remove(subtask_id)
        crawler_map[crawler_ip].mark_idle()
    elif status == "ERROR":
        subtask.mark_error()
        crawler_map[crawler_ip].assigned_subtasks.remove(subtask_id)
        crawler_map[crawler_ip].mark_idle()


def start_crawler_update_listener():
    channel.basic_consume(
        queue="crawler_updates",
        on_message_callback=handle_crawler_updates,
        auto_ack=True,
    )
    channel.start_consuming()


def master_process(crawlers: List[Crawler]):
    logging.info("Master process started.")
    for c in crawlers:
        crawler_map[c.ip] = c

    Thread(target=start_crawler_update_listener, daemon=True).start()

    while True:
        response = sqs.receive_message(
            QueueUrl=queue_url, MaxNumberOfMessages=1, WaitTimeSeconds=10
        )

        messages = response.get("Messages", [])
        logging.info(f"messages: {messages}")
        if messages:
            for message in messages:
                try:
                    body = json.loads(message["Body"])
                    seed_urls = body.get("seeds", [])
                    depth = body.get("depth", 1)

                    logging.info(f"New task: {len(seed_urls)} seeds, depth={depth}")
                    task = Task(seed_urls)
                    task_map[task.id] = task
                    task.create_subtasks()

                    for subtask in task.subtasks:
                        subtask_map[subtask.id] = subtask
                        crawler = get_least_loaded_crawler(crawlers)
                        if crawler:
                            subtask.assign_to_crawler(crawler.ip)
                            crawler.assign_task(subtask.id)
                            channel.basic_publish(
                                exchange="crawler_exchange",
                                routing_key=crawler.ip,
                                body=f"{subtask.id}|{subtask.url}|{crawler.ip}",
                            )
                            logging.info(f"Dispatched {subtask.id} to {crawler.ip}")

                    sqs.delete_message(
                        QueueUrl=queue_url, ReceiptHandle=message["ReceiptHandle"]
                    )
                except Exception as e:
                    logging.error(f"Failed to process SQS message: {e}")
        time.sleep(1)


if __name__ == "_main_":
    crawlers = [
        Crawler("172.31.84.194"),
    ]
    master_process(crawlers)
    connection.close()
