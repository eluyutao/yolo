from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from data_downloader.list_tickers import get_ticker_for_day
# from airflow.utils.trigger_rule import TriggerRule
# import pandas_market_calendars as mcal
from airflow.macros import ds_add


default_args = {
    'owner': 'Lucas',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

@dag(dag_id='daily_stock_downloader_v11', 
     default_args=default_args, 
     start_date=datetime(2023, 4, 21), 
     schedule_interval='@daily')
def daily_etl():

    def get_ds():
        # ds is the logical date (or execution date), which is the interval start time + interval
        # e.g. if interval is 04-02 00:00:00 to 04-02 23:59:59, then logical date is 04-03 00:00:00 to ensure capture all the data in 04-02
        return get_current_context()['ds']
    
    @task()
    def get_daily_stock_list():
        dt = ds_add(get_ds(),-1)
        print(f"get_daily_stock is running!!!!!!!!!! today's date is " + dt)
        return get_ticker_for_day(dt)

    @task()
    def weekly():
        print('weekly 123123123123123')
        pass

    @task(trigger_rule='one_success')
    def start_download(stock_list):
        print("start_download running")
        pass
        # cnt = 0
        # for x in stock_list:
        #     print(x)
        #     cnt += 1
        #     if cnt >=2:
        #         break

    # stock_list = 
    td = start_task('2023-01-01')
    br = branch_func(dt=td)
    dly = get_daily_stock_list()
    wly = weekly()
    dl = start_download('stock_list')
  
    td >> br >> [dly, wly] >> dl

greet_dag = daily_etl()



