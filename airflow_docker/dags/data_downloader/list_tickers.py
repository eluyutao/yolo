from polygon import RESTClient
import pandas as pd
from tqdm import tqdm
from datetime import datetime
try:
    from data_downloader.utils import *
except:
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

def get_ticker_price(ticker, freq, est_start_time, est_end_time, multiplier=1, limit=5000):
    '''
    freq: "minute", "day.."
    '''
    start_time = est_ts_2_utc_unix_milli(est_start_time)
    end_time   = est_ts_2_utc_unix_milli(est_end_time)
    aggs = client.get_aggs(ticker, multiplier, freq, start_time, end_time, limit=limit)
    tmp_lst = []
    for bar in aggs:
        row = [ticker,
                utc_unix_milli_2_est_ts(bar.timestamp),
                bar.open, bar.high, bar.low, bar.close,
                bar.volume, bar.vwap, bar.transactions, bar.otc]
        tmp_lst.append(row)
    # time_col = 'dt' if freq == 'day' else 'ts'
    df = pd.DataFrame(tmp_lst, columns=['ticker', 'date_time', 'open_price', 'high_price', 'low_price','close_price', 
                                        'volume', 'vwap', 'transactions', 'otc'])
    return df

if __name__ == '__main__':
    # aggs = get_ticker_price('AAPL', "minute", '2023-04-20 09:30:01', '2023-04-24 09:33:00')
    # aggs2 = get_ticker_price('AAPL', "minute", '2023-04-19', '2023-04-24 20:00:00')
    aggs2 = get_ticker_price('A', "day", '2023-04-24', '2023-04-25')
    aggs3 = get_ticker_price('A', "day", '2023-04-25', '2023-04-26')

    # get_ticker_for_day('2023-04-19')
