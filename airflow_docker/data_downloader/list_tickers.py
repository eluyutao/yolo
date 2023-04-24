from polygon import RESTClient
import pandas as pd
from tqdm import tqdm



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


if __name__ == '__main__':
    tickerList = get_ticker_for_day('2023-01-01')
    print(len(tickerList))