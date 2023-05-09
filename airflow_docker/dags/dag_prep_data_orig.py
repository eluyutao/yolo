from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

# from airflow.utils.trigger_rule import TriggerRule
# import pandas_market_calendars as mcal
from airflow.macros import ds_add

from tqdm import tqdm
import pendulum
import pandas as pd

from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
# from airflow.example_dags.plugins.workday import AfterWorkdayTimetable

import sys
sys.path.append('./../plugins')
from workday import AfterWorkdayTimetable



try:
    from data_downloader.utils import insert_do_nothing_on_conflicts
    from data_downloader.list_tickers import get_ticker_for_day, get_ticker_price
except:
    from utils import insert_do_nothing_on_conflicts
    from list_tickers import get_ticker_for_day, get_ticker_price

from pandas.tseries.holiday import USFederalHolidayCalendar

DB_CONN_ID = 'postgres_win_remote'

default_args = {
    'owner': 'Lucas',
    'retries': 5,
    'retry_delay': timedelta(minutes=1),
    'depends_on_past':False
}

def get_ts():
    '''
    if dag start_date = pendulum.datetime(2020, 1, 1, tz="US/Eastern"), and schedule_interval='0 22 * * Mon-Fri'
    then 
        Data interval start = 2020-01-01, 22:00:00 EST
        Data interval end = 2020-01-02, 22:00:00 EST,
        utc_time: 2020-01-02T03:00:00+00:00
        est_time: 2020-01-01T22:00:00-05:00
    '''
    utc_time = get_current_context()['ts']
    # est_time = pendulum.from_format(utc_time, 'YYYY-MM-DDTHH:MM:SS', tz='US/Eastern')
    est_time = pendulum.parse(utc_time).in_tz('US/Eastern')

    # print('------------------- get_ts() function - ', f'utc_time: {utc_time}, est_time: {est_time}')
    return str(est_time)

@task()
def get_daily_stock_list():
    ts = get_ts() 
    dt = ds_add(ts[:10], 1) # 01-01
    # dt = ts[:10] # 01-01

    print(f" ---------------- get_daily_stock() is running for {dt}")
    return get_ticker_for_day(dt)

@task(
        # trigger_rule='one_success'
      )
def start_download(stock_list, dim='day'):
    assert dim in ('day', 'minute'), "wrong input!"
    if dim=='day':
        tb_nm = 'stock_price_daily'
    elif dim=='minute':
        tb_nm = 'stock_price_minute'

    ts = get_ts()
    start_time = ds_add(ts[:10], 1)
    end_time = ds_add(ts[:10], 2)

    holiday_calendar = USFederalHolidayCalendar()
    hols = [str(x)[:10] for x in holiday_calendar.holidays('2010-01-01','2050-12-31')]
    if end_time in hols:
        print(f'--------- {end_time} is a holiday')
        return

    postgres_hook = PostgresHook(postgres_conn_id=DB_CONN_ID, schema='datadl')
    read_sql_df = postgres_hook.get_pandas_df(sql=f'''select exists(
                                                        select 1
                                                        from {tb_nm} 
                                                        where date(date_time) = to_date('{start_time}', 'YYYY-MM-DD')
                                                        and ticker in ('TSLA', 'GOOG', 'SPY')
                                                    ) as exists
                                                ''')

    # print('read_sql_df.iloc[0,0] is : ', read_sql_df.iloc[0,0], 'with type ', type(read_sql_df.iloc[0,0]))
    if read_sql_df.iloc[0,0]:
        print(f'--------- database already has record for date {start_time}')
        return
    
    print(f"start_download is running!!!!!!!!!! logical timestamp is " + ts)
    
    df_lst = [pd.DataFrame([])]
    for s in tqdm(stock_list, position=0, leave=True):
        print(f'start_time (est) is {start_time}',
                f'end_time (est) is {end_time}')
        df = get_ticker_price(s, dim, start_time, end_time)
        df_lst.append(df)

    df_all = pd.concat(df_lst, ignore_index=True)

    # if no data is returned for all tickers on that day (due to holiday), then don't do anything
    if len(df_all) > 0:
        postgres_hook = PostgresHook(postgres_conn_id=DB_CONN_ID, schema='datadl')
        print(f"---------------- start writing {df_all.ticker.nunique()} tickers on date {list(df_all.date_time.astype(str).str[:10].unique())} to postegresql database ----------------")
        df_all.to_sql(tb_nm, postgres_hook.get_sqlalchemy_engine(), 
                    if_exists='append', index=False, chunksize=5000,
                    method=insert_do_nothing_on_conflicts)
        print("---------------- finished writing table to postegresql database ----------------")
        



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

@dag(dag_id='minute_stock_downloader_v0002', 
     default_args=default_args, 
     start_date=pendulum.datetime(2020, 1, 1, tz="US/Eastern"), 
     schedule_interval='0 22 * * Sun-Thu',
    #  schedule = AfterWorkdayTimetable(),
     concurrency=24,
     max_active_runs=24)
def minute_price_dl():
    create_minute_postgres_table = PostgresOperator(
            task_id='create_minute_postgres_table',
            postgres_conn_id=DB_CONN_ID,
            sql="""
                create table if not exists stock_price_minute (
                    ticker character varying(10) not null,
                    date_time timestamp with time zone not null,
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
    dl = start_download(stock_list, 'minute')
  
    stock_list >> create_minute_postgres_table >> dl



daydl = day_price_dl()
mindl = minute_price_dl()



if __name__ == "__main__":
    greet_dag = daily_etl()
    greet_dag.test(pendulum.datetime(2023, 4, 24, 22, 0, 0, tz="US/Eastern"))