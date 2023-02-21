# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from pymongo import MongoClient

class CryptoNewsParserPipeline:
    def __init__(self):
        client = MongoClient('localhost:27017')
        self.mongo_db = client.db_crypto_data

    def process_item(self, item, spider):
        collection = self.mongo_db[spider.name]
        if collection.find_one({'news_title': item['news_title']}) is None:
            collection.insert_one(item)

        return item
