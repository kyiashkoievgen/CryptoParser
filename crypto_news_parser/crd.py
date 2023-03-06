import argparse
import datetime

import pandas as pd

from get_data import DataFromDB

data_format = "%d-%m-%Y"
db = DataFromDB()

parser = argparse.ArgumentParser(description='Search some information in DB and store in csv file')
parser.add_argument('-f', '--file', dest='filename', metavar='filename', nargs='*', action='store')
parser.add_argument('-b', '--begin', metavar='dd-mm-yyyy',
                    dest='begin', action='store',
                    help='Start finding time dd-mm-yyyy')
parser.add_argument('-e', '--end', metavar='dd-mm-yyyy',
                    dest='end', action='store',
                    help='Finish searching time dd-mm-yyyy')
parser.add_argument('-i', '--interval', dest='interval', action='store', default=1,
                    help='interval between date, s')
parser.add_argument('-n', '--news', dest='news', action='store_true',
                    help='store news in file')
parser.add_argument('-k', '--klines', dest='klines', action='store_true',
                    help='store klines in file')
parser.add_argument('-t', '--trades', dest='trades', action='store_true',
                    help='store trades in file')
parser.add_argument('-d', '--depth', dest='depth', action='store_true',
                    help='store depth in file')
args = parser.parse_args()
start_time = 0
stop_time = int(datetime.datetime.now().timestamp())
filename = 0
if args.filename:
    filename = args.filename[0]
else:
    filename = ''

if args.begin:
    start_time = int(datetime.datetime.strptime(args.begin, data_format).timestamp())
if args.end:
    stop_time = int(datetime.datetime.strptime(args.end, data_format).timestamp())
if args.news:
    df = db.get_news_by_time(start_time, stop_time)
    df.to_csv(f'~/crypto_data/news_{args.begin}_{args.end}'+filename+'.csv')

if args.trades:
    df = db.get_traders(start_time, stop_time)
    df.to_csv(f'~/crypto_data/trades_{args.begin}_{args.end}'+filename+'.csv')

if args.depth:
    df = db.get_depth(start_time, stop_time)
    df.to_csv(f'~/crypto_data/depth_{args.begin}_{args.end}'+filename+'.csv')

if args.klines or (not args.depth and not args.trades) and not args.news:
    df = db.get_klines(start_time, stop_time)
    df.to_csv(f'~/crypto_data/klines_{args.begin}_{args.end}'+filename+'.csv')


    print(df)
