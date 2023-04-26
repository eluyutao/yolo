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
        context = get_current_context()
        ds = context['ds']
        print(type(ds))
        return ds

    @task(task_id="get_current_date")
    def start_task(dt):
        print(f'yesterday is {ds_add(get_ds(),-1)}')
        return get_ds()

    @task.branch(task_id="evaluate_market_status")
    def branch_func(dt):
        # xcom_value = int(ti.xcom_pull(task_ids="start_task"))
        if dt >= 5:
            return "get_daily_stock_list" # run just this one task, skip all else
        elif dt >0 :
            return "weekly"
        else:
            return None # skip everything

    
    @task()
    def get_daily_stock_list():

        print(f"get_daily_stock is running!!!!!!!!!! today's date is" + get_ds())
        # return get_ticker_for_day(dt)

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



