import time
from urllib.parse import urlencode

import scrapy
from scrapy.http import HtmlResponse
from crypto_news_parser.items import BinanceApiParserItem

class BinanceApiSpider(scrapy.Spider):
    name = "binance_api"
    allowed_domains = ["api.binance.com"]
    start_urls = ["https://api.binance.com/api/v3/depth?symbol=BTCUSDT",
                  "https://api.binance.com/api/v3/trades?symbol=BTCUSDT",
                  "https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1s"
                  ]

    def parse(self, response: HtmlResponse):
        yield BinanceApiParserItem(
            add_item=response.json(),
            add_url=response.url
        )
        # print("response content: ########################################   \n",
        #       response.url,
        #       response.text,
        #       '\n !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
        time.sleep(1)
        for url in self.start_urls:
            yield response.follow(url=url, callback=self.parse, dont_filter=True)

