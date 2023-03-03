import scrapy
from scrapy.http import HtmlResponse
import re
from scrapy.loader import ItemLoader
from pymongo import MongoClient
import datetime

from crypto_news_parser.items import CryptoNewsParserItem


class BinanceSpider(scrapy.Spider):
    name = "binance"
    allowed_domains = ["binance.com"]
    start_urls = ["https://www.binance.com/en/news"]
    binance_pages = 0

    def parse(self, response: HtmlResponse):

        if response.xpath("// div[contains(text(), 'No records found.')]").get() is None:
            tag_data = re.findall(r'(<div[^>]*>\d\d\d\d-\d\d-\d\d</div>)', response.text)
            data_tag_class = re.findall(r'class=[^>]*', tag_data[0])
            news_day = response.xpath(f'//div[@ {data_tag_class[0]}]')
            for block_day in news_day:
                news_block = block_day.xpath("./../../div/div/div")
                news_data = block_day.xpath("./text()").get()
                for news in news_block:
                    loader = ItemLoader(item=CryptoNewsParserItem(), response=response)
                    news_time_news = news.xpath("./div")
                    news_time = news_data + ' ' + news_time_news[0].xpath("./text()").get()
                    dt_fmt = '%Y-%m-%d %H:%M'
                    news_time = int(datetime.datetime.strptime(news_time, dt_fmt).timestamp())
                    news_title = news_time_news[1].xpath(".//a/div")[0].xpath("./text()").get()
                    news_text = news_time_news[1].xpath(".//a/div/div/text()").get()

                    loader.add_value('news_title', news_title)
                    loader.add_value('news_text', news_text)
                    loader.add_value('news_time', news_time)
                    yield loader.load_item()
            BinanceSpider.binance_pages += 1
            yield response.follow(BinanceSpider.start_urls[0] + f'?page={self.binance_pages}', callback=self.parse)



