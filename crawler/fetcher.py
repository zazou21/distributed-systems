import logging
import requests
from bs4 import BeautifulSoup
from typing import Optional, Dict, Any
import urllib.robotparser
from urllib.parse import urlparse
import time
from urllib.parse import urljoin


class RobotsHandler:
    def __init__(self, user_agent: str = "MyCrawler/1.0"):
        self.user_agent = user_agent
        self.parsers = {}  # domain -> RobotFileParser

    def is_allowed(self, url: str) -> bool:
        parsed_url = urlparse(url)
        domain = parsed_url.netloc

        if domain not in self.parsers:
            robots_url = f"{parsed_url.scheme}://{domain}/robots.txt"
            rp = urllib.robotparser.RobotFileParser()
            try:
                rp.set_url(robots_url)
                rp.read()
                self.parsers[domain] = rp
                time.sleep(0.2)  # be polite when requesting robots.txt
                logging.info(f"Parsed robots.txt for {domain}")
            except Exception as e:
                logging.warning(f"Couldn't read robots.txt from {robots_url}: {e}")
                self.parsers[domain] = None  # fallback: allow all

        parser = self.parsers.get(domain)
        if parser:
            return parser.can_fetch(self.user_agent, url)
        return True  # default: allow


class PageFetcher:
    def __init__(self, robots_handler: RobotsHandler, headers=None):
        self.headers = headers or {"User-Agent": "MyCrawler/1.0"}
        self.robots_handler = robots_handler

    def fetch_page(self, url: str) -> Optional[str]:
        try:
            response = requests.get(url, headers=self.headers, timeout=10)
            if response.status_code == 200:
                logging.info(f"Fetched page: {url}")
                return response.text
            logging.warning(
                f"Failed to fetch {url} - Status Code: {response.status_code}"
            )
        except Exception as e:
            logging.error(f"Error fetching {url}: {e}")
        return None

    def extract_content(self, html: str, base_url: str) -> Dict[str, Any]:
        soup = BeautifulSoup(html, "html.parser")
        main_content = (
            soup.find("article")
            or soup.find("div", id="mw-content-text")
            or soup.find("div", role="main")
        )
        text = (
            main_content.get_text(separator=" ", strip=True)[:10000]
            if main_content
            else soup.get_text(separator=" ", strip=True)[:10000]
        )
        links = []
        for tag in soup.find_all("a", href=True):
            href = tag["href"]
            abs_url = urljoin(base_url, href)
            if abs_url.startswith("http") and self.robots_handler.is_allowed(abs_url):
                links.append(abs_url)
            elif not self.robots_handler.is_allowed(abs_url):
                print(f"Blocked by robots.txt: {abs_url}")

        return {"text": text, "extracted_urls": links}
        # return links
