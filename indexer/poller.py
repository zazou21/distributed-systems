import boto3
import json
import logging
import time


class SQSPoller:
    def __init__(self, queue_url):
        self.queue_url = queue_url
        self.sqs = boto3.client("sqs", region_name="us-east-1")

    def poll(self, handler, stop_event=None):
        logging.info("Starting SQS polling loop...")
        while True:
            if stop_event and stop_event.is_set():
                break
            try:
                response = self.sqs.receive_message(
                    QueueUrl=self.queue_url,
                    MaxNumberOfMessages=5,
                    WaitTimeSeconds=10,
                )
                for msg in response.get("Messages", []):
                    body = json.loads(msg["Body"])
                    handler(body["url"], body["text"])
                    self.sqs.delete_message(
                        QueueUrl=self.queue_url, ReceiptHandle=msg["ReceiptHandle"]
                    )
            except Exception as e:
                logging.error(f"Polling error: {e}")
            time.sleep(1)
