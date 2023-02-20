import scrapy
from scrapy.http import HtmlResponse
from pymongo import MongoClient
from crypto_news_parser.items import CryptoNewsParserItem


class BinanceSpider(scrapy.Spider):
    name = "binance"
    allowed_domains = ["binance.com"]
    start_urls = ["https://www.binance.com/en/news"]
    binance_pages = 0

    #     for i in range(BinanceSpider.binance_history_days):
    #         cur_time = datetime.date( time.localtime().tm_year, time.localtime().tm_mon, time.localtime().tm_mday)
    #         datetime.timedelta(datetime.timedelta(days=10), days=5)
    #         time_days_ago = time.localtime()
    #         year_now = time_now.tm_year
    #         month_now = time_now.tm_mon
    #         day_now = time_now.tm_mday
    #
    #         collection.find({'month': {'$gt': month_now}})
    #         collection.fing({'day': {'$gt': day_now-1}})

    def parse(self, response: HtmlResponse):
        client = MongoClient('localhost:27017')
        mongo_db = client.db_crypto_data
        collection = mongo_db[BinanceSpider.name]
        news_block = response.xpath('//div[@ class ="css-azayts"]/..')
        if response.xpath("// div[contains(text(), 'No records found.')]").get() is None:
            date_index = 0
            for block in news_block:
                news_title = block.xpath("//div[@ class = 'css-b5absw']/text()").getall()
                news_text = block.xpath("//div[@ class = 'css-1tktcmi']/text()").getall()
                news_time = block.xpath("//div[@ class = 'css-1x54tey']/text()").getall()
                news_date = block.xpath("//div[@ class = 'css-11yeefv']/text()").getall()
                i = 0
                for title in news_title:
                    if collection.find_one({'news_title': title}) is None:
                        if news_time[i].find('ago') <= 0:
                            document = {
                                'news_date': news_date[date_index],
                                'news_title': title,
                                'news_text': news_text[i],
                                'news_time': news_time[i]
                            }
                            collection.insert_one(document)
                            print(document)
                        i += 1
                    # else:
                    #     news_inside_db = True
                    #     break
                date_index += 1
            #(not news_inside_db) and (
            BinanceSpider.binance_pages += 1
            yield response.follow(BinanceSpider.start_urls[0] + f'?page={self.binance_pages}', callback=self.parse)
