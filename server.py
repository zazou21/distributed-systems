from flask import Flask, request, jsonify
from flask_cors import CORS
from flask_socketio import SocketIO, emit
import boto3
import logging
import os
import uuid
import json
import threading
import time
from elasticsearch import Elasticsearch, exceptions

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - Web Server - %(levelname)s - %(message)s"
)

app = Flask(__name__)
CORS(app)
socketio = SocketIO(app, cors_allowed_origins="*")

sqs = boto3.client("sqs", region_name="us-east-1")
client_to_task_map = {}  # task_id -> sid (socket session id)

es = Elasticsearch(
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
    logging.info(f"Connected to Elasticsearch: {es.info()}")
except Exception as e:
    logging.error(f"Failed to connect to Elasticsearch: {str(e)}")
crawl_queue_url = (
    "https://sqs.us-east-1.amazonaws.com/022499012946/first-sqs-queue.fifo"
)
master_server_queue_url = (
    "https://sqs.us-east-1.amazonaws.com/022499012946/queue-master-server.fifo"
)


@app.route("/start-crawl", methods=["POST"])
def start_crawl():
    seeds = request.json.get("seeds", [])
    depth = request.json.get("depth", 1)
    task_id = str(uuid.uuid4())

    try:
        message_body = {"seeds": seeds, "depth": depth, "task_id": task_id}
        response = sqs.send_message(
            QueueUrl=crawl_queue_url,
            MessageBody=json.dumps(message_body),
            MessageGroupId="crawl-group",
            MessageDeduplicationId=str(uuid.uuid4()),
        )
        logging.info(
            f"Sent task {task_id} to master | MessageId: {response.get('MessageId')}"
        )
    except Exception as e:
        logging.error(f"Failed to send task to master: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

    return jsonify({"status": "queued", "task_id": task_id})


@app.route("/search", methods=["POST"])
def search():
    data = request.get_json()
    if not data or "query" not in data:
        return jsonify({"error": "Missing 'query' in request body"}), 400

    query = data["query"].strip()
    if not query:
        return jsonify({"error": "Query cannot be empty"}), 400

    try:
        response = es.search(
            index="webpages",
            body={
                "query": {
                    "match": {
                        "text": query,
                    }
                },
                "highlight": {
                    "fields": {
                        "text": {
                            "number_of_fragments": 2,
                            "fragment_size": 150,
                        }
                    }
                },
            },
        )
        hits = response.get("hits", {}).get("hits", [])
        results = []
        for hit in hits:
            url = hit["_source"].get("url")
            snippet = hit.get("highlight", {}).get("text", [""])[0]
            results.append({"url": url, "snippet": snippet})
        return jsonify({"results": results})
    except Exception as e:
        logging.error(f"Elasticsearch search error: {str(e)}")
        return jsonify({"error": str(e)}), 500


@socketio.on("connect")
def on_connect():
    logging.info(f"Client connected: {request.sid}")


@socketio.on("disconnect")
def on_disconnect():
    logging.info(f"Client disconnected: {request.sid}")
    # Clean up mappings
    to_remove = [tid for tid, sid in client_to_task_map.items() if sid == request.sid]
    for tid in to_remove:
        del client_to_task_map[tid]


@socketio.on("register_task")
def on_register_task(data):
    task_id = data.get("task_id")
    if task_id:
        client_to_task_map[task_id] = request.sid
        logging.info(f"Registered client {request.sid} for task {task_id}")


def master_update_listener():
    logging.info("Started master -> server listener")
    while True:
        try:
            response = sqs.receive_message(
                QueueUrl=master_server_queue_url,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=10,
                AttributeNames=["All"],
            )
            messages = response.get("Messages", [])
            for message in messages:
                body = json.loads(message["Body"])
                task_id = body.get("task_id")
                status = body.get("status")

                if task_id in client_to_task_map:
                    sid = client_to_task_map[task_id]
                    socketio.emit(
                        "task_update", {"task_id": task_id, "status": status}, room=sid
                    )
                    logging.info(
                        f"Sent status update to client for task {task_id}: {status}"
                    )

                sqs.delete_message(
                    QueueUrl=master_server_queue_url,
                    ReceiptHandle=message["ReceiptHandle"],
                )
            time.sleep(1)
        except Exception as e:
            logging.error(f"Master update listener error: {str(e)}")
            time.sleep(5)


if __name__ == "__main__":
    threading.Thread(
        target=master_update_listener, daemon=True, allow_unsafe_werkzeug=True
    ).start()
    socketio.run(app, host="0.0.0.0", port=5000)
