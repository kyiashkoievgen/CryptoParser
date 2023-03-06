from pymongo import MongoClient
import pandas as pd


def get_text(cursor, news:{}):
    for el in cursor:
        text = el['news_text']
        date = el['news_time']
        if len(text) > 0:
            if type(date) == list:
                date = date[0]
            if news.get(date):
                news[date].append(text[0])
            else:
                news[date] = text
    return news


class DataFromDB:
    def __init__(self):
        client = MongoClient('127.0.0.1:27017')
        mongo_db = client.db_crypto_data
        self.binance_news_collection = mongo_db['binance']
        self.cryptonews_collection = mongo_db['cryptonews']
        self.depth_collection = mongo_db['depth']
        self.klines_collection = mongo_db['klines']
        self.trades_collection = mongo_db['trades']

    def get_news_by_time(self, from_time, to_time):
        news = {}
        cursor = self.binance_news_collection.find(
            {
                'news_time': {'$gt': from_time},
                'news_time': {'$lt': to_time}
            }
        )
        news = get_text(cursor, news)

        cursor = self.cryptonews_collection.find(
            {
                'news_time': {'$gt': from_time},
                'news_time': {'$lt': to_time}
            }
        )
        news = get_text(cursor, news)
        date = news.keys()
        text = []
        for each_date in date:
            text.append(news[each_date])
        return pd.DataFrame({'time': date, 'news': text})

    def get_traders(self, from_time, to_time):
        cursor = self.trades_collection.find(
            {
                'add_time.serverTime': {'$gt': from_time*1000},
                'add_time.serverTime': {'$lt': to_time*1000}
            }
        )
        data = {
            'Trade_time': [],
            'isBuyer': [],
            'Price': [],
            'Qty': [],

        }
        for each_server_time in cursor:
            for each_buyer in each_server_time['add_item']:
                data['Trade_time'].append(each_buyer['time'])
                data['isBuyer'].append(each_buyer['isBuyerMaker'])
                data['Price'].append(each_buyer['price'])
                data['Qty'].append(each_buyer['qty'])

        return pd.DataFrame(data)

    def get_depth(self, from_time, to_time):
        cursor = self.depth_collection.find(
            {
                'add_time.serverTime': {'$gt': from_time * 1000},
                "add_time.serverTime": {'$lt': to_time * 1000}
            }
        )
        data = {
            'Depth_time':[]
        }
        for i in range(100):
            data['Bids_Price_'+str(i)] = []
            data['Bids_qty_'+str(i)] = []
            data['Asks_Price_'+str(i)] = []
            data['Asks_qty_'+str(i)] = []

        for each_time in cursor:
            data['Depth_time'].append(each_time['add_time']['serverTime'])
            for i in range(100):
                data['Bids_Price_' + str(i)].append(each_time['add_item']['bids'][i][0])
                data['Bids_qty_' + str(i)].append(each_time['add_item']['bids'][i][1])
                data['Asks_Price_' + str(i)].append(each_time['add_item']['asks'][i][0])
                data['Asks_qty_' + str(i)].append(each_time['add_item']['asks'][i][1])

        return pd.DataFrame(data)

    def get_klines(self, from_time, to_time):
        cursor = self.klines_collection.find(
            {
                'add_time.serverTime': {'$gt': from_time * 1000},
                "add_time.serverTime": {'$lt': to_time * 1000}
            }
        )
        data = {
            'Klines_time': [],
            'Open_price': [],
            'High_price': [],
            'Low_price': [],
            'Close_price': [],
            'Volume': [],
            'Quote_asset_volume': [],
            'Number_of_trades': [],
            'Taker_buy_base_asset_volume' : [],
            'Taker_buy_quote_asset_volume' : []
        }
        for each_time in cursor:
            for each_klines in each_time['add_item']:
                data['Klines_time'].append(each_klines[0])
                data['Open_price'].append(each_klines[1])
                data['High_price'].append(each_klines[2])
                data['Low_price'].append(each_klines[3])
                data['Close_price'].append(each_klines[4])
                data['Volume'].append(each_klines[5])
                data['Quote_asset_volume'].append(each_klines[7])
                data['Number_of_trades'].append(each_klines[8])
                data['Taker_buy_base_asset_volume'].append(each_klines[9])
                data['Taker_buy_quote_asset_volume'].append(each_klines[10])

        return pd.DataFrame(data)