# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class CryptoNewsParserItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    news_title = scrapy.Field()
    news_text = scrapy.Field()
    news_time = scrapy.Field()
    _id = scrapy.Field()

