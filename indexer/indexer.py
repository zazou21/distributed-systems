import json
import time
import threading
import logging
import boto3
from elasticsearch import Elasticsearch, exceptions

logging.basicConfig(level=logging.INFO)
logging.getLogger("elasticsearch").setLevel(logging.WARNING)


# ElasticIndexer class using custom analyzer
class ElasticIndexer:
    def __init__(self, index_name="webpages"):
        self.index_name = index_name
        self.es = Elasticsearch(
            hosts=[{"host": "172.31.92.167", "port": 9200, "scheme": "https"}],
            basic_auth=("elastic", "sl_ZN64F4UN4uoDw5AIC"),
            verify_certs=False,
        )
        try:
            print(self.es.info())
        except Exception as e:
            print("Failed to connect:", e)

        if not self.es.ping():
            raise RuntimeError(" Elasticsearch ping failed. Check URL/auth.")
        logging.info("Elasticsearch cluster is up!")

        settings = {
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


# IndexerNode class using SQS and ElasticIndexer
class IndexerNode:
    def __init__(self, queue_url):
        self.queue_url = queue_url
        self.sqs = boto3.client("sqs", region_name="us-east-1")
        self.indexer = ElasticIndexer()

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
                    body = json.loads(msg["Body"])
                    self.indexer.add_document(body["url"], body["text"])
                    logging.info(f"Indexed document from URL: {body['url']}")
                    self.sqs.delete_message(
                        QueueUrl=self.queue_url, ReceiptHandle=msg["ReceiptHandle"]
                    )
                    logging.info(f"Deleted message from SQS for URL: {body['url']}")
            except Exception as e:
                logging.error(f"Error while polling messages: {e}")
            time.sleep(1)

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
    node.cli()
