import pika
from pymongo import MongoClient
import threading
import json
import logging

# MongoDB Setup (thread-safe)
# Inside your Python script
mongo_client = MongoClient(
    "mongodb://mongo1:27017,mongo2:27017/?replicaSet=rs0"  # Use Docker service names
)
db = mongo_client["distributedDB"]
subtasks = db["subtasks"]
tasks = db["tasks"]
crawlers = db["crawlers"]
indexers = db["indexers"]

# RabbitMQ Setup
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logging.getLogger("pika").setLevel(logging.WARNING)
queues = ["crawlers", "indexers", "master", "task_registry", "subtask_registry"]

RABBITMQ_HOST = "172.31.81.152"
RABBITMQ_PORT = 5672
RABBITMQ_CREDS = pika.PlainCredentials("admin", "admin")
HEARTBEAT_INTERVAL = 300  # 5 minutes
CONNECTION_TIMEOUT = 300  # 5 minutes

params = pika.ConnectionParameters(
    host=RABBITMQ_HOST,
    port=RABBITMQ_PORT,
    credentials=RABBITMQ_CREDS,
    heartbeat=HEARTBEAT_INTERVAL,
    blocked_connection_timeout=CONNECTION_TIMEOUT,
    socket_timeout=10,
)


def handle_tasks(ch, method, properties, body):
    try:
        message = json.loads(body.decode())
        logging.info(f"[task] received")

        # Check if the task exists and update or insert if it doesn't
        task_id = message.get("id")  # Assuming the unique identifier is 'id'
        if task_id:
            tasks.update_one(
                {"id": task_id},  # Match by 'id'
                {"$set": message},  # Update the fields with the new data
                upsert=True,  # If the task doesn't exist, create it
            )
        else:
            logging.error("[TASK] No ID found in the message.")

        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        logging.error(f"[TASK] Error: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def handle_subtasks(ch, method, properties, body):
    try:
        message = json.loads(body.decode())
        logging.info(f"[SUBTASK] received")

        # Check if the subtask exists and update or insert if it doesn't
        subtask_id = message.get(
            "subtask_id"
        )  # Assuming the unique identifier is 'subtask_id'
        if subtask_id:
            subtasks.update_one(
                {"subtask_id": subtask_id},  # Match by 'subtask_id'
                {"$set": message},  # Update the fields with the new data
                upsert=True,  # If the subtask doesn't exist, create it
            )
        else:
            logging.error("[SUBTASK] No subtask_id found in the message.")

        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        logging.error(f"[SUBTASK] Error: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def handle_crawlers(ch, method, properties, body):
    try:
        message = json.loads(body.decode())
        logging.info(f"[CRAWLER] received")

        # Check if the crawler exists and update or insert if it doesn't
        crawler_id = message.get("crawler_ip")  # Assuming the unique identifier is 'id'
        if crawler_id:
            crawlers.update_one(
                {"id": crawler_id},  # Match by 'id'
                {"$set": message},  # Update the fields with the new data
                upsert=True,  # If the crawler doesn't exist, create it
            )
        else:
            logging.error("[CRAWLER] No ID found in the message.")

        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        logging.error(f"[CRAWLER] Error: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def handle_indexers(ch, method, properties, body):
    try:
        message = json.loads(body.decode())
        logging.info(f"[INDEXER] received")

        # Check if the indexer exists and update or insert if it doesn't
        indexer_id = message.get("indexer_ip")  # Assuming the unique identifier is 'id'
        if indexer_id:
            indexers.update_one(
                {"id": indexer_id},  # Match by 'id'
                {"$set": message},  # Update the fields with the new data
                upsert=True,  # If the indexer doesn't exist, create it
            )
        else:
            logging.error("[INDEXER] No ID found in the message.")

        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        logging.error(f"[INDEXER] Error: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def handle_master(ch, method, properties, body):
    try:
        message = json.loads(body.decode())
        indexers = message.get("indexers", [])
        crawlers = message.get("crawlers", [])
        tasks = message.get("tasks", [])
        subtasks = message.get("subtasks", [])
        print(f"[MASTER] Received: {message}")
        db.indexers.insert_many(indexers)
        db.crawlers.insert_many(crawlers)
        db.tasks.insert_many(tasks)
        db.subtasks.insert_many(subtasks)
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print(f"[MASTER] Error: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


# Mapping queue name to callback
callback_map = {
    "crawlers": handle_crawlers,
    "indexers": handle_indexers,
    "master": handle_master,
    "task_registry": handle_tasks,
    "subtask_registry": handle_subtasks,
}


# Start consuming from each queue in a separate thread
def start_listener(queue_name):
    connection = pika.BlockingConnection(params)

    channel = connection.channel()
    channel.queue_declare(queue=queue_name, durable=False)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(
        queue=queue_name, on_message_callback=callback_map[queue_name]
    )
    print(f"[x] Listening on queue: {queue_name}")
    channel.start_consuming()


# Start threads
for queue in queues:
    threading.Thread(target=start_listener, args=(queue,), daemon=True).start()

# Keep main thread alive
try:
    while True:
        pass
except KeyboardInterrupt:
    print("Shutting down...")
