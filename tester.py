# # Import the necessary classes from crawler and indexer modules
# from crawler.fetcher import RobotsHandler, PageFetcher
# from indexer.indexer_node import IndexerNode
# from indexer.elastic_search import ElasticIndexer

# # robots_handler = RobotsHandler()
# # fetcher = PageFetcher(robots_handler=robots_handler)


# # page = fetcher.fetch_page("https://en.wikipedia.org/wiki/Computer_science")
# # content = fetcher.extract_content(
# #     page, "https://en.wikipedia.org/wiki/Computer_science"
# # )


# indexer = ElasticIndexer()

# # indexer.add_document("https://en.wikipedia.org/wiki/Computer_science", content["text"])

# response = indexer.es.search(index="webpages", query={"match": {"text": "distributed"}})

# # Print the results
# for hit in response["hits"]["hits"]:
#     print(f"Score: {hit['_score']}, URL: {hit['_source']['url']}")
#     print(f"Text: {hit['_source']['text'][:200]}...\n")

# # print("Document added to indexer.")
from elasticsearch import Elasticsearch

# Initialize connection
es = Elasticsearch(
    hosts=[{"host": "localhost", "port": 9200, "scheme": "https"}],
    basic_auth=("elastic", "new_password"),
    verify_certs=False,
)


def search_keyword(keyword):
    response = es.search(
        index="webpages",
        query={"match": {"text": keyword}},
        size=10,  # adjust as needed
    )

    hits = response["hits"]["hits"]
    if not hits:
        print("No results found.")
    else:
        print(f"Found {len(hits)} result(s):")
        for hit in hits:
            print(f"- {hit['_source'].get('url')}")


if __name__ == "__main__":
    while True:
        keyword = input(
            "\nEnter a keyword to search (or type 'exit' to quit): "
        ).strip()
        if keyword.lower() == "exit":
            break
        search_keyword(keyword)
