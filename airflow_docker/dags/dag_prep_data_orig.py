from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from data_downloader.list_tickers import get_ticker_for_day, get_ticker_price
# from airflow.utils.trigger_rule import TriggerRule
# import pandas_market_calendars as mcal
from airflow.macros import ds_add

from tqdm import tqdm
import pendulum


default_args = {
    'owner': 'Lucas',
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
}

@dag(dag_id='daily_stock_downloader_v35', 
     default_args=default_args, 
     start_date=pendulum.datetime(2023, 4, 24, tz="US/Eastern"), 
     schedule_interval='0 22 * * Mon-Fri')
def daily_etl():

    def get_ts():
        # ds is the logical date (or execution date), which is the interval start time + interval
        # e.g. if interval is 04-02 00:00:00 to 04-02 23:59:59, then logical date is 04-03 00:00:00 to ensure capture all the data in 04-02
        return get_current_context()['ts']
    
    @task()
    def get_daily_stock_list():
        ts = get_ts()
        dt = ds_add(ts[:10],-1)
        print(f"get_daily_stock is running!!!!!!!!!! logical timestamp is " + ts)
        return get_ticker_for_day(dt)

    @task(trigger_rule='one_success')
    def start_download(stock_list):
        print("start_download running")

        ts = get_ts()
        start_time = ds_add(ts[:10],-1)
        end_time = ts[:10]
        for s in tqdm(stock_list[:3], position=0, leave=True):
            print(f'start_time (est) is {start_time}',
                  f'end_time (est) is {end_time}')
            df = get_ticker_price(s, 'day', start_time, end_time)
            # print('finished one loop')



    stock_list = get_daily_stock_list()
    dl = start_download(stock_list)
  
    stock_list >> dl

greet_dag = daily_etl()



