# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from pymongo import MongoClient
import re
import datetime, time

class CryptoNewsParserPipeline:
    def __init__(self):
        client = MongoClient('127.0.0.1:27017')
        self.mongo_db = client.db_crypto_data

    def process_item(self, item, spider):
        url = ''
        if item.get('add_url'):
            url = item.pop('add_url')
        if spider.name == 'binance_api':
            if url.find('depth') > 0:
                collection = self.mongo_db['depth']
                collection.insert_one(item)
            elif url.find('trades') > 0:
                collection = self.mongo_db['trades']
                collection.insert_one(item)
            elif url.find('klines') > 0:
                collection = self.mongo_db['klines']
                collection.insert_one(item)
        else:
            collection = self.mongo_db[spider.name]
            if collection.find_one({'news_title': item['news_title']}) is None:
                collection.insert_one(item)
        # print('###############',
        #       spider,
        #       item,
        #       '#############')

        return item
