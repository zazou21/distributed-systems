from dataclasses import dataclass


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
