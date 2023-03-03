import scrapy


class BinanceApiSpider(scrapy.Spider):
    name = "binance_api"
    allowed_domains = ["ws-api.binance.com"]
    start_urls = ["http://ws-api.binance.com/"]

    def parse(self, response):
        pass
