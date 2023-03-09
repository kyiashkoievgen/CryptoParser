# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import os

# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from pymongo import MongoClient
import pandas as pd
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
        elif spider.name == 'cryptonews' or spider.name == 'daly_cryptonews' or spider.name == 'binance':
            collection = self.mongo_db[spider.name]
            if collection.find_one({'news_title': item['news_title']}) is None:
                collection.insert_one(item)
        # print('###############',
        #       spider,
        #       item,
        #       '#############')

        return item


class CryptoNewsParserPipelineToFile:
    def __init__(self):
        if not os.path.isfile('~/BTC_depth_hist.csv'):
            df = {'Depth_time': []}
            for i in range(100):
                df['Bids_Price_' + str(i)] = []
                df['Bids_qty_' + str(i)] = []
                df['Asks_Price_' + str(i)] = []
                df['Asks_qty_' + str(i)] = []
            pd.DataFrame(df).to_csv('~/BTC_depth_hist.csv')
        if not os.path.isfile('~/BTC_trade_hist.csv'):
            df_data = {
                'id': [],
                'price': [],
                'qty': [],
                'time': [],
                'isBuyerMaker': []
            }
            pd.DataFrame(df_data).to_csv('~/BTC_trade_hist.csv')

    def process_item(self, item, spider):
        url = ''
        if item.get('add_url'):
            url = item.pop('add_url')
        if spider.name == 'binance_to_file':
            if url.find('depth') > 0:
                df_data = {'Depth_time': [item['add_time']['serverTime']]}
                i = 0
                for each_bids in item['add_item']['bids']:
                    df_data[f'Bids_Price_{str(i)}'] = [each_bids[0]]
                    df_data[f'Bids_qty_{str(i)}'] = [each_bids[1]]
                    i += 1
                i = 0
                for each_asks in item['add_item']['asks']:
                    df_data[f'Asks_Price_{str(i)}'] = [each_asks[0]]
                    df_data[f'Asks_qty_{str(i)}'] = [each_asks[1]]
                    i += 1

                pd.DataFrame(df_data).to_csv('~/BTC_depth_hist.csv', mode='a', index=False, header=False)

            elif url.find('trades') > 0:
                df_data = {
                    'id': [],
                    'price': [],
                    'qty': [],
                    'time': [],
                    'isBuyerMaker': []
                }
                for each_trade in item['add_item']:
                    df_data['id'].append(each_trade['id'])
                    df_data['price'].append(each_trade['price'])
                    df_data['qty'].append(each_trade['qty'])
                    df_data['time'].append(each_trade['time'])
                    df_data['isBuyerMaker'].append(each_trade['isBuyerMaker'])
