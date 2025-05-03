from . import elastic_search
from . import poller
import threading


class IndexerNode:
    def __init__(self, queue_url):
        self.indexer = elastic_search.ElasticIndexer()
        self.poller = poller.SQSPoller(queue_url)

    def start(self):
        threading.Thread(
            target=self.poller.poll, args=(self.indexer.add_document,), daemon=True
        ).start()
