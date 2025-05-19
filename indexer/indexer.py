import json
import time
import threading
import logging
from typing import Any, Dict, Optional
import boto3
from elasticsearch import Elasticsearch, exceptions
from dataclasses import dataclass, field
import pika
import socket


logging.basicConfig(level=logging.INFO)
logging.getLogger("pika").setLevel(logging.WARNING)
logging.getLogger("elasticsearch").setLevel(logging.WARNING)


@dataclass
class IndexerConfig:
    rabbitmq_host: str = "172.31.81.152"
    rabbitmq_port: int = 5672
    rabbitmq_username: str = "admin"
    rabbitmq_password: str = "admin"
    sqs_queue_url: str = (
        "https://sqs.us-east-1.amazonaws.com/022499012946/crawler-indexer-sqs.fifo"
    )
    elastic_search_settings: dict = field(
        default_factory=lambda: {
            "settings": {
                "analysis": {
                    "analyzer": {
                        "custom_english_analyzer": {
                            "type": "custom",
                            "tokenizer": "standard",
                            "filter": ["lowercase", "english_stop", "english_stemmer"],
                        }
                    },
                    "filter": {
                        "english_stop": {"type": "stop", "stopwords": "_english_"},
                        "english_stemmer": {"type": "stemmer", "language": "english"},
                    },
                }
            },
            "mappings": {
                "properties": {
                    "url": {"type": "keyword"},
                    "text": {"type": "text", "analyzer": "custom_english_analyzer"},
                }
            },
        }
    )


# ElasticIndexer class using custom analyzer
class ElasticIndexer:
    def __init__(self, index_name="webpages"):
        self.index_name = index_name
        self.es = Elasticsearch(
            hosts=[
                "http://172.31.92.167:9200",
                "http://172.31.86.187:9200",
                "http://172.31.90.42:9200",
            ],
            sniff_on_start=True,
            sniff_on_connection_fail=True,
            sniffer_timeout=30,
        )
        try:
            print(self.es.info())
        except Exception as e:
            print("Failed to connect:", e)

        if not self.es.ping():
            raise RuntimeError(" Elasticsearch ping failed. Check URL/auth.")
        logging.info("Elasticsearch cluster is up!")

        settings = IndexerConfig().elastic_search_settings
        try:
            resp = self.es.indices.create(
                index=self.index_name,
                body=settings,
                ignore=400,
            )
            if (
                resp.get("acknowledged")
                or resp.get("error", {}).get("type")
                == "resource_already_exists_exception"
            ):
                logging.info(f" Index '{self.index_name}' ready.")
            else:
                logging.warning(f" Unexpected create response: {resp}")
        except exceptions.ElasticsearchException as e:
            logging.error(" Failed to create index:", e)
            raise

    def add_document(self, url, text):
        self.es.index(index=self.index_name, document={"url": url, "text": text})
        logging.info(f"Document indexed: {url}")

    def search(self, keyword):
        query = {"query": {"match": {"text": keyword}}}
        response = self.es.search(index=self.index_name, body=query)
        hits = response.get("hits", {}).get("hits", [])
        return [hit["_source"]["url"] for hit in hits]


class Reporter:
    def __init__(self, config: IndexerConfig):
        self.config = config

    def _get_connection(self):
        return pika.BlockingConnection(
            pika.ConnectionParameters(
                host=self.config.rabbitmq_host,
                port=self.config.rabbitmq_port,
                credentials=pika.PlainCredentials(
                    self.config.rabbitmq_username, self.config.rabbitmq_password
                ),
                heartbeat=300,
                blocked_connection_timeout=300,
            )
        )

    def report(self, queue: str, body: Dict[str, Any]):
        """Thread-safe message reporting"""
        try:
            connection = self._get_connection()
            channel = connection.channel()
            channel.queue_declare(queue=queue, durable=False)
            channel.basic_publish(exchange="", routing_key=queue, body=json.dumps(body))
            connection.close()
        except Exception as e:
            logging.error(f"Failed to report to {queue}: {e}")

    def report_status(
        self, subtask_id: str, indexer_ip: str, status: str, data: Optional[Dict] = None
    ):
        self.report(
            "indexer_updates",
            {
                "subtask_id": subtask_id,
                "indexer_ip": indexer_ip,
                "status": status,
                "data": data or {},
            },
        )


# IndexerNode class using SQS and ElasticIndexer
class IndexerNode:
    def __init__(self, queue_url):
        self.queue_url = queue_url
        self.ip = socket.gethostbyname(socket.gethostname())
        self.sqs = boto3.client("sqs", region_name="us-east-1")
        self.indexer = ElasticIndexer()
        self.config = IndexerConfig()
        self.reporter = Reporter(self.config)

    def handle_document(self, msg):
        try:

            body = json.loads(msg["Body"])
            url = body.get("url")
            text = body.get("text")
            subtask_id = body.get("subtask_id")
            start_time = time.time()

            self.reporter.report_status(
                indexer_ip=self.ip, subtask_id=subtask_id, status="PROCESSING"
            )

            if url and text:

                self.indexer.add_document(url, text)

                logging.info(f"Document added to index: {url}")

                end_time = time.time()
                indexing_time = end_time - start_time

                self.reporter.report_status(
                    indexer_ip=self.ip,
                    subtask_id=subtask_id,
                    status="DONE",
                    data={"indexing_time": indexing_time},
                )

            else:
                logging.warning("Invalid message format. Missing 'url' or 'text'.")
                self.reporter.report_status(
                    indexer_ip=self.ip, subtask_id=subtask_id, status="ERROR"
                )
        except json.JSONDecodeError as e:
            logging.error(f"Failed to decode message body: {e}")
        except Exception as e:
            logging.error(f"Error handling document: {e}")

    def poll_messages(self):
        while True:
            try:
                response = self.sqs.receive_message(
                    QueueUrl=self.queue_url,
                    MaxNumberOfMessages=5,
                    WaitTimeSeconds=10,
                    MessageAttributeNames=["All"],
                )
                messages = response.get("Messages", [])
                if messages:
                    logging.info(f"Received {len(messages)} messages from SQS")
                for msg in messages:
                    self.handle_document(msg)
                    self.sqs.delete_message(
                        QueueUrl=self.queue_url, ReceiptHandle=msg["ReceiptHandle"]
                    )

            except Exception as e:
                logging.error(f"Error while polling messages: {e}")
            time.sleep(1)

    def _heartbeat_loop(self):
        logging.info("Starting heartbeat loop")
        reporter = Reporter(self.config)
        while True:
            try:
                reporter.report(
                    "indexer_registry",
                    {"queue_name": self.ip, "timestamp": time.time()},
                )
                logging.info(f"Heartbeat sent from {self.ip}")
                time.sleep(15)
            except Exception as e:
                logging.error(f"Heartbeat failed: {e}")
                time.sleep(5)

    def start(self):
        logging.info("IndexerNode started. Polling for messages...")
        threading.Thread(target=self.poll_messages, daemon=True).start()

    def cli(self):
        while True:
            keyword = input("Enter keyword to search (or 'exit'): ")
            if keyword.lower() == "exit":
                logging.info("Exiting CLI...")
                break
            results = self.indexer.search(keyword)
            if results:
                logging.info(f"Search results for '{keyword}': {results}")
            else:
                logging.info(f"No results found for keyword: {keyword}")


if __name__ == "__main__":
    queue_url = (
        "https://sqs.us-east-1.amazonaws.com/022499012946/crawler-indexer-sqs.fifo"
    )
    node = IndexerNode(queue_url)
    node.start()
    threading.Thread(target=node._heartbeat_loop, daemon=True).start()
    node.poll_messages()
