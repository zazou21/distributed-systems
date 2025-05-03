import boto3
import json
import logging


class IndexerClient:
    def __init__(self, queue_url):
        self.sqs = boto3.client("sqs", region_name="us-east-1")
        self.queue_url = queue_url

    def send_to_indexer(self, url, text):
        message = {"url": url, "text": text}
        self.sqs.send_message(
            QueueUrl=self.queue_url,
            MessageBody=json.dumps(message),
            MessageGroupId="crawler-group",
            MessageDeduplicationId=str(hash(url)),
        )
        logging.info(f"Sent data for {url} to indexer")
