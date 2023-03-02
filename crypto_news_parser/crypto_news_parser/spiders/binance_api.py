import time
from urllib.parse import urlencode

import scrapy
from scrapy.http import HtmlResponse
from crypto_news_parser.items import BinanceApiParserItem

class BinanceApiSpider(scrapy.Spider):
    name = "binance_api"
    allowed_domains = ["api.binance.com"]
    start_urls = ["https://api.binance.com/api/v3/time"]
    api_urls = [
        "https://api.binance.com/api/v3/depth?symbol=BTCUSDT",
        "https://api.binance.com/api/v3/trades?symbol=BTCUSDT",
        "https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1s"
        ]

    binance_time = 0

    def parse(self, response: HtmlResponse):
        self.binance_time = response.json()
        #['serverTime']
        for url in self.api_urls:
            yield response.follow(url=url, callback=self.api_parse, dont_filter=True)

    def api_parse(self, response: HtmlResponse):
        yield BinanceApiParserItem(
            add_item=response.json(),
            add_url=response.url,
            add_time=self.binance_time
        )
        # print("response content: ########################################   \n",
        #       response.url,
        #       response.text,
        #       '\n !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
        time.sleep(1)
        yield response.follow(url=self.start_urls[0], callback=self.parse, dont_filter=True)

