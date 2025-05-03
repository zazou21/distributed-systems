import logging
import pika
import json
import socket
import time

from fetcher import PageFetcher
from reporter import Reporter
from indexer_client import IndexerClient


class CrawlerNode:
    def __init__(self, config):
        self.config = config
        self.my_ip = socket.gethostbyname(socket.gethostname())
        self.fetcher = PageFetcher()
        self.reporter = Reporter(config)
        self.indexer = IndexerClient(config.sqs_queue_url)

    def _setup_rabbitmq(self):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                self.config.rabbitmq_host,
                self.config.rabbitmq_port,
                credentials=pika.PlainCredentials(
                    self.config.rabbitmq_username, self.config.rabbitmq_password
                ),
            )
        )
        self.channel = self.connection.channel()
        self.channel.exchange_declare(
            exchange="crawler_exchange", exchange_type="direct"
        )
        queue_name = f"queue_{self.my_ip}"
        self.channel.queue_declare(queue=queue_name)
        self.channel.queue_bind(
            exchange="crawler_exchange", queue=queue_name, routing_key=self.my_ip
        )
        self.channel.basic_consume(
            queue=queue_name, on_message_callback=self._handle_message, auto_ack=True
        )

    def _handle_message(self, ch, method, properties, body):
        message = body.decode()
        if message == "ping":
            self.reporter.report_status("ping", self.my_ip, "pong")
            return

        subtask_id, url, crawler_ip = message.split("|")
        self.reporter.report_status(subtask_id, crawler_ip, "PROCESSING")
        html = self.fetcher.fetch_page(url)
        time.sleep(self.config.crawl_delay)

        if not html:
            self.reporter.report_status(
                subtask_id, crawler_ip, "ERROR", {"reason": "fetch failed"}
            )
            return

        content = self.fetcher.extract_content(html, url)
        self.indexer.send_to_indexer(url, content["text"])
        self.reporter.report_status(
            subtask_id, crawler_ip, "DONE", {"url": url, **content}
        )

    def run(self):
        try:
            self._setup_rabbitmq()
            logging.info(f"Crawler {self.my_ip} running...")
            self.channel.start_consuming()
        except KeyboardInterrupt:
            self.stop()
        except Exception as e:
            logging.error(f"Fatal error: {e}")
            self.stop()

    def stop(self):
        if self.channel:
            self.channel.stop_consuming()
        if self.connection:
            self.connection.close()
        self.reporter.close()
