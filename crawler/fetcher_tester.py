import fetcher

robots_handler = fetcher.RobotsHandler()
fetcher = fetcher.PageFetcher(robots_handler=robots_handler)


page = fetcher.fetch_page("https://en.wikipedia.org/wiki/Computer_science")

result = fetcher.extract_content(page, "https://en.wikipedia.org/wiki/Computer_science")
