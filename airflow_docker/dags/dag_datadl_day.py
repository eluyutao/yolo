from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

# from airflow.utils.trigger_rule import TriggerRule
# import pandas_market_calendars as mcal


import pendulum
import pandas as pd

from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
# from airflow.example_dags.plugins.workday import AfterWorkdayTimetable

import sys
sys.path.append('./../plugins')
from workday import AfterWorkdayTimetable

from common_tasks import get_daily_stock_list, start_download, DB_CONN_ID


default_args = {
    'owner': 'Lucas',
    'retries': 5,
    'retry_delay': timedelta(minutes=1),
    'depends_on_past':False
}



@dag(dag_id='day_stock_downloader_v0002', 
     default_args=default_args, 
     start_date=pendulum.datetime(2020, 1, 1, tz="US/Eastern"), 
     schedule_interval='0 22 * * Sun-Thu',
    #  schedule = AfterWorkdayTimetable(),
     concurrency=24,
     max_active_runs=24)
def day_price_dl():
    create_daily_postgres_table = PostgresOperator(
            task_id='create_daily_postgres_table',
            postgres_conn_id=DB_CONN_ID,
            sql="""
                create table if not exists stock_price_daily (
                    id SERIAL,
                    ticker character varying(10) not null,
                    date_time date not null,
                    open_price numeric,
                    high_price numeric,
                    low_price numeric,
                    close_price numeric,
                    volume numeric,
                    vwap numeric,
                    transactions int,
                    otc boolean,
                    PRIMARY KEY (ticker, date_time),
                    UNIQUE(ticker, date_time)
                );
            """
        )

    stock_list = get_daily_stock_list()
    dl = start_download(stock_list, 'day')
  
    stock_list >> create_daily_postgres_table >> dl



daydl = day_price_dl()



if __name__ == "__main__":
    # greet_dag = daily_etl()
    # greet_dag.test(pendulum.datetime(2023, 4, 24, 22, 0, 0, tz="US/Eastern"))
    pass