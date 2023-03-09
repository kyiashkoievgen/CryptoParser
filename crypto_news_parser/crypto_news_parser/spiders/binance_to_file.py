import scrapy
from scrapy.http import HtmlResponse
from crypto_news_parser.item import BinanceToFileParserItem

import time


class BinanceToFileSpider(scrapy.Spider):
    name = "binance_to_file"
    allowed_domains = ["api.binance.com"]
    start_urls = ["https://api.binance.com/api/v3/time"]
    binance_time = 0

    def parse(self, response: HtmlResponse):
        self.binance_time = response.json()

        yield response.follow("https://api.binance.com/api/v3/depth?symbol=BTCUSDT", callback=self.depth,
                              dont_filter=True)
        yield response.follow("https://api.binance.com/api/v3/trades?symbol=BTCUSDT", callback=self.trades,
                              dont_filter=True)

    def depth(self, response):
        yield BinanceToFileParserItem(
            add_item=response.json(),
            add_time=self.binance_time,
            add_url=response.url
        )
        time.sleep(1)
        yield response.follow("https://api.binance.com/api/v3/time", callback=self.parse, dont_filter=True)

    def trades(self, response):
        yield BinanceToFileParserItem(
            add_item=response.json(),
            add_time=self.binance_time,
            add_url=response.url
        )
