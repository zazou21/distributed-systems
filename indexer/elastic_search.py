import logging
from elasticsearch import Elasticsearch, exceptions


logging.basicConfig(level=logging.INFO)


class ElasticIndexer:
    def __init__(self, index_name="webpages"):
        self.index_name = index_name
        self.es = Elasticsearch(
            hosts=[{"host": "localhost", "port": 9200, "scheme": "https"}],
            basic_auth=("elastic", "sl_ZN64F4UN4uoDw5AIC"),
            verify_certs=False,
        )
        try:
            print(self.es.info())
        except Exception as e:
            print("Failed to connect:", e)

        # 1) Ping the cluster
        if not self.es.ping():
            raise RuntimeError("❌ Elasticsearch ping failed. Check URL/auth.")
        logging.info("✅ Elasticsearch cluster is up!")

        # 2) Create index with custom analyzer, ignore 400 (already exists)
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
                ignore=400,  # ← swallow “index already exists” errors
            )
            if (
                resp.get("acknowledged")
                or resp.get("error", {}).get("type")
                == "resource_already_exists_exception"
            ):
                logging.info(f"✅ Index '{self.index_name}' ready.")
            else:
                logging.warning(f"⚠️ Unexpected create response: {resp}")
        except exceptions.ElasticsearchException as e:
            logging.error("❌ Failed to create index:", e)
            raise

    def add_document(self, url, text):
        """Index a raw document; ES will apply your custom analyzer."""
        self.es.index(index=self.index_name, document={"url": url, "text": text})
        logging.info(f"Document indexed: {url}")
