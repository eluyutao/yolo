from polygon import RESTClient
import pandas as pd
from tqdm import tqdm
from datetime import datetime
from utils import *




client = RESTClient("CtCiOlTjiJGq_5NLZNQbMM6Y0GExYsJ5") # api_key is used

def get_ticker_for_day(dt):
    exchanges = pd.DataFrame(client.get_exchanges(asset_class='stocks',
                                                locale='us'))

    exchangeList = list(set(exchanges.mic))
    exchangeList.remove(None)

    # client.get_ticker_types(asset_class='stocks', locale='us')
    # below is what I have chosen. refer to the reference.xlsx file
    ticker_types = ['CS','ETF','ADRC','FUND']

    usTickers = []
    for type in tqdm(ticker_types):
        for ex in exchangeList:
            for t in client.list_tickers(type=type,
                                            market='stocks',
                                            exchange=ex,
                                            date=dt,
                                            active=True,
                                            limit=1000,
                                            sort='ticker'):
                usTickers.append(t.ticker)

    # final ticker list
    finalTickerList = sorted(list(set(usTickers)))
    return finalTickerList

def get_ticker_price(ticker, freq, est_start_time, est_end_time):
    '''
    freq: "minute", "day.."
    '''
    start_time = est_ts_2_utc_unix_milli(est_start_time)
    end_time   = est_ts_2_utc_unix_milli(est_end_time)
    aggs = client.get_aggs(ticker, 1, freq, start_time, end_time, limit=5000)
    tmp_lst = []
    for bar in aggs:
        row = [bar.timestamp,
                utc_unix_milli_2_est_ts(bar.timestamp / 1000),
                bar.open, bar.high, bar.low, bar.close,
                bar.volume, bar.vwap, bar.transactions, bar.otc]
        tmp_lst.append(row)
    df = pd.DataFrame(tmp_lst, columns=['unix_ts', 'timestamp', 'open', 'high', 'low','close', 'volume', 'vwap', 'transactions', 'otc'])
    

if __name__ == '__main__':
    aggs = client.get_aggs('AAPL', 1, "minute", '2023-04-23', '2023-04-24', limit=5000)
    print(type(aggs))
    for a in aggs:
        print(a)