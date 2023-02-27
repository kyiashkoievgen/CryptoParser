# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy
from itemloaders.processors import TakeFirst


class CryptoNewsParserItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    news_title = scrapy.Field(output_processor=TakeFirst())
    news_text = scrapy.Field(output_processor=TakeFirst())
    news_time = scrapy.Field(output_processor=TakeFirst())
    _id = scrapy.Field()


class BinanceApiParserItem(scrapy.Item):
    add_item = scrapy.Field()
    add_url = scrapy.Field()
    _id = scrapy.Field()


