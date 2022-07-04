import pandas as pd
from pyfinviz.screener import Screener

# get list of filtered tickers from finviz.com
page = 186

options = [Screener.IndustryOption.STOCKS_ONLY_EX_FUNDS, 
           Screener.MarketCapOption.SMALL_OVER_USD300MLN]
screener = Screener(filter_options=options,
                    pages=[x for x in range(1, page)])

list_ticker = []
for i in range(0, page):
    if i == 1:
        pass
    else:
        for j in range(len(screener.data_frames[i])):
            list_ticker.append(screener.data_frames[i].Ticker[j])
list_ticker = sorted(list(set(list_ticker)))