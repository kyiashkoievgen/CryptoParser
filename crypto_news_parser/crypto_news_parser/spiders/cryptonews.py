import datetime
import re
import time

import scrapy
from scrapy.http import HtmlResponse
from crypto_news_parser.items import CryptoNewsParserItem


class CryptonewsSpider(scrapy.Spider):
    name = "cryptonews"
    allowed_domains = ["cryptonews.net"]
    start_urls = ["https://cryptonews.net/?page=496)"]

    def parse(self, response: HtmlResponse):
        news_link = response.xpath('//a[@ class="title"]')
        next_page = response.xpath('//a[@ class="show-more"]')
        if next_page:
            yield response.follow(next_page[0], callback=self.parse)

        for news in news_link:
            yield response.follow(news, callback=self.parse_news)

    def parse_news(self, response: HtmlResponse):
        news_title = response.xpath("//h1/text()").get()
        news_text = response.xpath('//div[@ class ="news-item detail content_text"]/p/text()').getall()
        news_time = response.xpath('//span[@ class ="datetime flex middle-xs"]/text()[last()]').get()

        obj = re.findall(r'(\d h)|(\d m)', news_time)
        if obj:
            if obj[0][1]:
                news_time = int(time.time())+int(re.findall(r'\d', ''.join(obj[0][1]))[0]*60)
            else:
                news_time = int(time.time())+int(re.findall(r'\d', ''.join(obj[0][0]))[0])*3600

        else:
            obj = re.split(r', ', news_time)
            if obj:
                dt_fmt = '%d %B %Y %H:%M'
                news_time = int(datetime.datetime.strptime(obj[0], dt_fmt).timestamp())

        yield CryptoNewsParserItem(
            news_title=news_title,
            news_text=news_text,
            news_time=news_time

        )

