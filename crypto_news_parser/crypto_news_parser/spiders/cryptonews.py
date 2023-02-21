import scrapy
from scrapy.http import HtmlResponse
from crypto_news_parser.items import CryptoNewsParserItem


class CryptonewsSpider(scrapy.Spider):
    name = "cryptonews"
    allowed_domains = ["cryptonews.net"]
    start_urls = ["https://cryptonews.net/"]

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
        yield CryptoNewsParserItem(
            news_title=news_title,
            news_text=news_text,
            news_time=news_time

        )

