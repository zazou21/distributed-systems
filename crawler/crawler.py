# crawler_node.py

import pika
import socket
import logging

# Logging setup
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def get_my_ip():
    """Utility to get this machine's IP (used as queue/routing key)."""
    hostname = socket.gethostname()
    return socket.gethostbyname(hostname)


def callback(ch, method, properties, body):
    try:
        subtask_id, url, crawler_ip = body.decode().split("|")
        logging.info(
            f"Received subtask:\n  ID: {subtask_id}\n  URL: {url}\n  For: {crawler_ip}"
        )
        # Add real crawling logic here
    except Exception as e:
        logging.error(f"Error processing message: {e}")


def main():
    my_ip = get_my_ip()  # Or hardcode for testing: my_ip = "192.168.1.10"

    # RabbitMQ connection
    connection = pika.BlockingConnection(pika.ConnectionParameters("EC2_PUBLIC_IP"))
    channel = connection.channel()

    # Declare exchange and bind queue
    channel.exchange_declare(exchange="crawler_exchange", exchange_type="direct")
    queue_name = f"queue_{my_ip}"
    channel.queue_declare(queue=queue_name)
    channel.queue_bind(exchange="crawler_exchange", queue=queue_name, routing_key=my_ip)

    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)

    logging.info(f"Crawler {my_ip} waiting for messages...")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        logging.info("Shutting down...")
        channel.stop_consuming()
    finally:
        connection.close()


if _name_ == "_main_":
    main()


crawler
