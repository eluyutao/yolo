from polygon import RESTClient
import sqlite3
from tqdm import tqdm

from datetime import datetime
from dateutil.relativedelta import relativedelta
import time
import pandas as pd


# time.sleep(60)


###### get list of SP500 symbols ######
data = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
table = data[0]
stock_list = table['Symbol'].values.tolist()

already_data_in_database = True
if already_data_in_database:
    connection = sqlite3.connect("E:\databases\yolo.db")
    cursor = connection.cursor()
    cursor.execute("select symbol, max(id) from stock_price_minute group by symbol")
    cur = cursor.fetchall()
    symbol_already_in_database = [x[0] for x in cur]
    max_id = max([x[1] for x in cur])
    i = max_id + 1
else:
    i = 1
    symbol_already_in_database = []

client = RESTClient("CtCiOlTjiJGq_5NLZNQbMM6Y0GExYsJ5") # api_key is used

api_call_num = 1
for ticker in tqdm(stock_list):
    if ticker not in symbol_already_in_database:
        two_yrs_ago = datetime.now() - relativedelta(years=2)
        start = two_yrs_ago
        end = datetime.now().date() - relativedelta(days=1)
        if api_call_num % 5 == 0:
            # print(f'{datetime.now()}... reached api call limit, waiting for 60 seconds...')
            time.sleep(60)
        aggs = client.get_aggs(ticker, 1, "minute", start, end, limit=50000)
        api_call_num += 1

        all_lst = []
        while 1:
            tmp_lst = []
            for bar in aggs:
                row = [bar.timestamp,
                        datetime.fromtimestamp(bar.timestamp / 1000),
                        bar.open, bar.high, bar.low, bar.close,
                        bar.volume, bar.vwap, bar.transactions, bar.otc]
                tmp_lst.append(row)
            df_tmp = pd.DataFrame(tmp_lst, columns=['timestamp', 'time', 'open', 'high', 'low','close', 'volume', 'vwap', 'transactions', 'otc'])
            all_lst.append(df_tmp)
            # print(df_tmp.shape)
            start = df_tmp['timestamp'].values[-1]

            # print(f'start date: {datetime.fromtimestamp(start / 1000).date()}, end date: {end}')
            if datetime.fromtimestamp(start / 1000).date() >= end:
                break

            if api_call_num % 5 == 0:
                # print(f'{datetime.now()}... reached api call limit, waiting for 60 seconds...')
                time.sleep(60)
            aggs = client.get_aggs(ticker, 1, "minute", start, end, limit=50000)
            api_call_num += 1
            
        df = pd.concat(all_lst, ignore_index=True).drop_duplicates()
        # df.index = df.time
        # df = df.resample('1min').ffill()
        # df = df[(df.time.astype(str).str[11:]>='04:00:00') & (df.time.astype(str).str[11:]<='19:59:00')]

        connection = sqlite3.connect("E:\databases\yolo.db")
        connection.row_factory = sqlite3.Row
        cursor = connection.cursor()
        
        for index, row in df.iterrows():
            cursor.execute("""
                INSERT INTO stock_price_minute (id, symbol, datetime, open, high, low, close, volume, trxn)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (i, ticker, row['time'].tz_localize(None).isoformat(), row['open'], row['high'], row['low'], row['close'], row['volume'], row['transactions']))
            i += 1

        connection.commit()