import pika
import socket
import logging
import requests
import time
import json
import boto3
from threading import Thread
from bs4 import BeautifulSoup
from dataclasses import dataclass
from typing import Optional, Dict, Any
from pymongo import MongoClient
import re
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser


# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

logging.getLogger("pika").setLevel(logging.WARNING)


blocked_patterns = [
    r".*login.*",  #  "login"
    r".*signup.*",  # "signup"
    r".*terms-of-service.*",  #  "terms-of-service"
    r".*privacy-policy.*",  #  "privacy-policy"
    r".*\.js$",  #  ".js"
    r".*\.css$",  #  ".css"
    r".*\.jpg$",  #  ".jpg"
    r".*\.png$",  #  ".png"
    r".*404.*",  # (error page)
    r".*403.*",  # (error page)
    r".*utm_.*",  # (tracking parameters)
]


def is_blocked(url):
    """Check if the URL matches any blocked patterns."""
    for pattern in blocked_patterns:
        if re.search(pattern, url):
            return True
    return False


@dataclass
class CrawlerConfig:
    crawl_delay: float = 1.0
    rabbitmq_host: str = "172.31.81.152"
    rabbitmq_port: int = 5672
    rabbitmq_username: str = "admin"
    rabbitmq_password: str = "admin"
    sqs_queue_url: str = (
        "https://sqs.us-east-1.amazonaws.com/022499012946/crawler-indexer-sqs.fifo"
    )


class PageFetcher:
    def __init__(self, config: CrawlerConfig):
        self.config = config
        self.headers = {"User-Agent": "MyCrawler/1.0"}

    def fetch_page(self, url: str) -> Optional[str]:
        try:
            response = requests.get(url, headers=self.headers, timeout=10)
            if response.status_code == 200:
                logging.info(f"Fetched page: {url}")
                return response.text
            logging.warning(
                f"Failed to fetch {url} - Status Code: {response.status_code}"
            )
        except Exception as e:
            logging.error(f"Error fetching {url}: {e}")
        return None

    def extract_content(self, html: str, base_url: str) -> Dict[str, Any]:
        soup = BeautifulSoup(html, "html.parser")
        main_content = (
            soup.find("article")
            or soup.find("div", id="mw-content-text")
            or soup.find("div", role="main")
        )
        text = (
            main_content.get_text(separator=" ", strip=True)[:10000]
            if main_content
            else soup.get_text(separator=" ", strip=True)[:10000]
        )

        links = list(
            {
                tag["href"]
                for tag in soup.find_all("a", href=True)
                if tag["href"].startswith("http")
            }
        )
        logging.info(f"Extracted {len(links)} links from {base_url}")

        return {"text": text, "extracted_urls": links}


class Reporter:
    def __init__(self, config: CrawlerConfig):
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
        self, subtask_id: str, crawler_ip: str, status: str, data: Optional[Dict] = None
    ):
        self.report(
            "crawler_updates",
            {
                "subtask_id": subtask_id,
                "crawler_ip": crawler_ip,
                "status": status,
                "data": data or {},
            },
        )

    def report_urls(
        self, new_urls: list[str], crawler_ip: str, parent_subtask_id: str, depth: int
    ):
        self.report(
            "crawler_urls",
            {
                "urls": new_urls,
                "crawler_ip": crawler_ip,
                "parent_subtask_id": parent_subtask_id,
                "depth": depth,
            },
        )
        logging.info(
            f"Reported {len(new_urls)} new URLs from {parent_subtask_id} at depth {depth}"
        )

    def report_db(self, subtask_id: str, url: str, html: str):
        self.report(
            "subtask_registry",
            {
                "subtask_id": subtask_id,
                "url": url,
                "html": html,
            },
        )
        logging.info(f"Reported HTML to DB for {subtask_id}")


class IndexerClient:
    def __init__(self, queue_url: str):
        self.sqs = boto3.client("sqs", region_name="us-east-1")
        self.queue_url = queue_url

    def send_to_indexer(self, url: str, text: str, subtask_id: str):
        try:
            self.sqs.send_message(
                QueueUrl=self.queue_url,
                MessageBody=json.dumps(
                    {"url": url, "text": text, "subtask_id": subtask_id}
                ),
                MessageGroupId="crawler-group",
                MessageDeduplicationId=str(hash(url)),
            )
        except Exception as e:
            logging.error(f"Failed to send to indexer: {e}")


class CrawlerNode:
    def __init__(self, config: CrawlerConfig):
        self.config = config
        self.queue_name = socket.gethostbyname(socket.gethostname())
        self.fetcher = PageFetcher(config)
        self.indexer = IndexerClient(config.sqs_queue_url)
        self.mongo_client = MongoClient("mongodb://172.31.82.224:27017")
        self.db = self.mongo_client["mongoDB"]
        self._shutdown_flag = False

    def _is_allowed_by_robots(self, url, domain):
        """Check if the URL is allowed by the robots.txt of the domain."""
        robot_url = f"http://{domain}/robots.txt"
        rp = RobotFileParser()
        rp.set_url(robot_url)
        rp.read()

        # Extract the path from the URL to check if it's disallowed
        path = urlparse(url).path
        return rp.can_fetch("*", path)

    def _filter_urls(self, urls, base_url):
        """Filter out URLs that are blocked or disallowed by robots.txt."""
        domain = urlparse(base_url).hostname

        filtered_urls = []
        for url in urls:
            if is_blocked(url):
                logging.info(f"Blocked URL: {url}")
                continue

            if not self._is_allowed_by_robots(url, domain):
                logging.info(f"Disallowed by robots.txt: {url}")
                continue

            filtered_urls.append(url)

        return filtered_urls

    def save_to_mongo(self, url: str, html: str, subtask_id: str):
        """Send the content to MongoDB queue."""
        reporter = Reporter(self.config)
        try:
            reporter.report_db(subtask_id, url, html)

        except Exception as e:
            logging.error(f"Failed to send to MongoDB: {e}")

    def _setup_rabbitmq(self):
        """Create a new connection for message consumption"""
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=self.config.rabbitmq_host,
                port=self.config.rabbitmq_port,
                credentials=pika.PlainCredentials(
                    self.config.rabbitmq_username, self.config.rabbitmq_password
                ),
                heartbeat=300,
            )
        )
        channel = connection.channel()
        channel.exchange_declare(
            exchange="crawler_exchange", exchange_type="direct", durable=False
        )
        channel.queue_declare(queue=f"queue_{self.queue_name}", durable=False)
        channel.queue_bind(
            exchange="crawler_exchange",
            queue=f"queue_{self.queue_name}",
            routing_key=self.queue_name,
        )
        return connection, channel

    def _handle_message(self, ch, method, properties, body):
        reporter = Reporter(self.config)

        try:
            subtask_id, url, crawler_ip, depth = body.decode().split("|")
            logging.info(f"Processing {subtask_id} for {url}")

            reporter.report_status(subtask_id, crawler_ip, "PROCESSING")

            start_time = time.time()  # Start timing

            html = self.fetcher.fetch_page(url)
            time.sleep(self.config.crawl_delay)

            if not html:
                reporter.report_status(
                    subtask_id, crawler_ip, "ERROR", {"reason": "fetch failed"}
                )
                return

            content = self.fetcher.extract_content(html, url)
            content["extracted_urls"] = self._filter_urls(
                content["extracted_urls"], url
            )

            try:
                self.save_to_mongo(url, html, subtask_id)
                logging.info("sent to mongo")
            except Exception as e:
                logging.error(f"MongoDB save failed: {e}")

            self.indexer.send_to_indexer(url, content["text"], subtask_id)
            logging.info(f"sent to indexer {subtask_id}")

            end_time = time.time()
            crawling_time = end_time - start_time  # Calculate duration

            reporter.report_status(
                subtask_id,
                crawler_ip,
                "DONE",
                {"url": url, "crawling_time": crawling_time, **content},
            )

            reporter.report_urls(
                content.get("extracted_urls", [])[:3],
                self.queue_name,
                subtask_id,
                int(depth) + 1,
            )
        except Exception as e:
            logging.error(f"Message processing failed: {e}")

    def _heartbeat_loop(self):
        logging.info("Starting heartbeat loop")
        reporter = Reporter(self.config)
        while not self._shutdown_flag:
            try:
                reporter.report(
                    "crawler_registry",
                    {"queue_name": self.queue_name, "timestamp": time.time()},
                )
                logging.info(f"Heartbeat sent from {self.queue_name}")
                time.sleep(15)
            except Exception as e:
                logging.error(f"Heartbeat failed: {e}")
                time.sleep(5)

    def run(self):
        Thread(target=self._heartbeat_loop, daemon=True).start()

        while not self._shutdown_flag:
            try:
                connection, channel = self._setup_rabbitmq()
                channel.basic_consume(
                    queue=f"queue_{self.queue_name}",
                    on_message_callback=self._handle_message,
                    auto_ack=True,
                )
                logging.info(f"Crawler {self.queue_name} started consuming")
                channel.start_consuming()
            except pika.exceptions.AMQPConnectionError:
                logging.error("RabbitMQ connection failed, retrying in 5s...")
                time.sleep(5)
            except Exception as e:
                logging.error(f"Unexpected error: {e}")
                time.sleep(1)
            finally:
                if "connection" in locals() and connection.is_open:
                    connection.close()

    def stop(self):
        self._shutdown_flag = True
        self.mongo_client.close()


if __name__ == "__main__":
    crawler = CrawlerNode(CrawlerConfig())
    try:
        crawler.run()
    except KeyboardInterrupt:
        crawler.stop()
