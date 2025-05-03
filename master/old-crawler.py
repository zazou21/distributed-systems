import pika
import socket
import logging
import requests
import time
import json
import boto3
from bs4 import BeautifulSoup
from dataclasses import dataclass
from typing import Optional, Dict, Any

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(                                                                                                             message)s')

@dataclass
class CrawlerConfig:
    master_ip: str = "EC2_PUBLIC_IP"
    crawl_delay: float = 1.0
    rabbitmq_host: str = "172.31.81.152"
    rabbitmq_port: int = 5672
    rabbitmq_username: str = "admin"
    rabbitmq_password: str = "admin"
    sqs_queue_url: str = "https://sqs.us-east-1.amazonaws.com/022499012946/crawl                                                                                                             er-indexer-sqs.fifo"

class PageFetcher:
    def __init__(self, config: CrawlerConfig):
        self.config = config
        self.headers = {'User-Agent': 'MyCrawler/1.0'}

    def fetch_page(self, url: str) -> Optional[str]:
        try:
            response = requests.get(url, headers=self.headers, timeout=10)
            if response.status_code == 200:
                logging.info(f"Fetched page: {url}")
                return response.text
            else:
                logging.warning(f"Failed to fetch {url} - Status Code: {response                                                                                                             .status_code}")
        except Exception as e:
            logging.error(f"Error fetching {url}: {e}")
        return None

    def extract_content(self, html: str, base_url: str) -> Dict[str, Any]:
        soup = BeautifulSoup(html, 'html.parser')
        content = {}

        # 1. Title and Meta Tags
        title = soup.find('title')
        content['title'] = title.get_text() if title else None

        description = soup.find('meta', attrs={'name': 'description'})
        content['description'] = description.get('content') if description else                                                                                                              None

        keywords = soup.find('meta', attrs={'name': 'keywords'})
        content['keywords'] = keywords.get('content') if keywords else None

        # 2. Headings (H1, H2, etc.)
        headings = {}
        for level in range(1, 7):  # h1 to h6
            headings[f'h{level}'] = [h.get_text(strip=True) for h in soup.find_a                                                                                                             ll(f'h{level}')]
        content['headings'] = headings

        # 3. Body Content (from p, article, and div)
        main_content = (
            soup.find('article') or
            soup.find('div', id='mw-content-text') or
            soup.find('div', role='main')
        )

        if main_content:
            content['text'] = main_content.get_text(separator=' ', strip=True)[:                                                                                                             10000]
        else:
            content['text'] = soup.get_text(separator=' ', strip=True)[:10000]

        # Collect all div content
        divs = soup.find_all('div')
        content['divs_text'] = [div.get_text(separator=' ', strip=True) for div                                                                                                              in divs]

        # 4. Links (Anchor Tags)
        links = []
        for tag in soup.find_all('a', href=True):
            href = tag['href']
            if href.startswith('http'):
                links.append(href)
        content['links'] = list(set(links))  # Remove duplicate links

        # 5. Images (Alt Text)
        images = []
        for img in soup.find_all('img', alt=True):
            img_url = img.get('src')
            img_alt = img.get('alt')
            if img_url:
                images.append({'src': img_url, 'alt': img_alt})
        content['images'] = images

        # 6. Structured Data (JSON-LD, Microdata, RDFa)
        structured_data = []
        json_ld = soup.find_all('script', type='application/ld+json')
        for js in json_ld:
            try:
                structured_data.append(json.loads(js.string))
            except Exception as e:
                logging.error(f"Error parsing JSON-LD: {e}")
        content['structured_data'] = structured_data

        # 7. Robots Meta Tags
        robots_meta = soup.find('meta', attrs={'name': 'robots'})
        if robots_meta and 'noindex' in robots_meta.get('content', '').lower():
            content['robots'] = 'noindex'
        else:
            content['robots'] = 'index'

        # 8. CSS/JS Files
        css_files = [link['href'] for link in soup.find_all('link', rel='stylesh                                                                                                             eet')]
        js_files = [script['src'] for script in soup.find_all('script', src=True                                                                                                             )]
        content['css_files'] = css_files
        content['js_files'] = js_files

        logging.info(f"Extracted content from {base_url}")

        return content

class Reporter:
    def __init__(self, config: CrawlerConfig):
        self.config = config
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(
            self.config.rabbitmq_host,
            self.config.rabbitmq_port,
            credentials=pika.PlainCredentials(self.config.rabbitmq_username, sel                                                                                                             f.config.rabbitmq_password)
        ))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue='crawler_updates')

    def report_status(self, subtask_id: str, crawler_ip: str, status: str, data:                                                                                                              Optional[Dict] = None):
        body = json.dumps({
            'subtask_id': subtask_id,
            'crawler_ip': crawler_ip,
            'status': status,
            'data': data or {}
        })
        self.channel.basic_publish(exchange='', routing_key='crawler_updates', b                                                                                                             ody=body)
        logging.info(f"Reported status {status} for subtask {subtask_id}")

    def close(self):
        self.connection.close()

class IndexerClient:
    def __init__(self, queue_url: str):
        self.sqs = boto3.client('sqs', region_name='us-east-1')
        self.queue_url = queue_url

    def send_to_indexer(self, url: str, text: str):
        message = {
            "url": url,
            "text": text
        }
        self.sqs.send_message(
            QueueUrl=self.queue_url,
            MessageBody=json.dumps(message),
            MessageGroupId="crawler-group",
            MessageDeduplicationId=str(hash(url))
        )
        logging.info(f"Sent data for {url} to indexer")

class CrawlerNode:
    def __init__(self, config: CrawlerConfig):
        self.config = config
        self.my_ip = self._get_my_ip()
        self.reporter = Reporter(config)
        self.fetcher = PageFetcher(config)
        self.indexer = IndexerClient(config.sqs_queue_url)
        self.connection = None
        self.channel = None

    def _get_my_ip(self) -> str:
        return socket.gethostbyname(socket.gethostname())

    def _setup_rabbitmq(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(
            self.config.rabbitmq_host,
            self.config.rabbitmq_port,
            credentials=pika.PlainCredentials(self.config.rabbitmq_username, sel                                                                                                             f.config.rabbitmq_password)
        ))
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange='crawler_exchange', exchange_type                                                                                                             ='direct')
        queue_name = f'queue_{self.my_ip}'
        self.channel.queue_declare(queue=queue_name)
        self.channel.queue_bind(exchange='crawler_exchange', queue=queue_name, r                                                                                                             outing_key=self.my_ip)
        self.channel.basic_consume(queue=queue_name, on_message_callback=self._h                                                                                                             andle_message, auto_ack=True)

    def _handle_message(self, ch, method, properties, body):
        body_decoded = body.decode()
        if body_decoded == "ping":
            logging.info("Received ping from master, responding with pong.")
            self.reporter.report_status("ping", self.my_ip, "pong")
            return

        subtask_id, url, crawler_ip = body_decoded.split("|")
        logging.info(f"Received subtask {subtask_id} for URL {url}")

        self.reporter.report_status(subtask_id, crawler_ip, "PROCESSING")

        html = self.fetcher.fetch_page(url)
        time.sleep(self.config.crawl_delay)

        if not html:
            self.reporter.report_status(subtask_id, crawler_ip, "ERROR", {"reaso                                                                                                             n": "fetch failed"})
            return

        content = self.fetcher.extract_content(html, url)
        logging.info(f"Extracted content for {url}")


        self.indexer.send_to_indexer(url, content['text'])
        self.reporter.report_status(subtask_id, crawler_ip, "DONE", {'url': url,                                                                                                              **content})

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

if __name__ == "__main__":
    config = CrawlerConfig()
    crawler = CrawlerNode(config)
    crawler.run()
