import pika
import json
import logging


class Reporter:
    def __init__(self, config):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                config.rabbitmq_host,
                config.rabbitmq_port,
                credentials=pika.PlainCredentials(
                    config.rabbitmq_username, config.rabbitmq_password
                ),
            )
        )
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue="crawler_updates")

    def report_status(self, subtask_id, crawler_ip, status, data=None):
        body = json.dumps(
            {
                "subtask_id": subtask_id,
                "crawler_ip": crawler_ip,
                "status": status,
                "data": data or {},
            }
        )
        self.channel.basic_publish(
            exchange="", routing_key="crawler_updates", body=body
        )
        logging.info(f"Reported status {status} for subtask {subtask_id}")

    def close(self):
        self.connection.close()
