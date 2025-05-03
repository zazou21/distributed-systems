import logging
from config import CrawlerConfig
from crawler_node import CrawlerNode

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )
    config = CrawlerConfig()
    crawler = CrawlerNode(config)
    crawler.run()
