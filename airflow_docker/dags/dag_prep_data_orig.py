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

try:
    from data_downloader.utils import insert_do_nothing_on_conflicts
    from data_downloader.list_tickers import get_ticker_for_day, get_ticker_price
except:
    from utils import insert_do_nothing_on_conflicts
    from list_tickers import get_ticker_for_day, get_ticker_price



default_args = {
    'owner': 'Lucas',
    'retries': 5,
    'retry_delay': timedelta(minutes=1),
    'depends_on_past':False
}

def get_ts():
    '''
    ds is the logical date (or execution date), which is the interval start time + interval
    e.g. if interval is 04-02 00:00:00 to 04-02 23:59:59, then logical date is 04-03 00:00:00 to ensure capture all the data in 04-02
    foramt of ts: 2023-04-25T02:00:00+00:00
    '''
    utc_time = get_current_context()['ts']
    # est_time = pendulum.from_format(utc_time, 'YYYY-MM-DDTHH:MM:SS', tz='US/Eastern')
    est_time = pendulum.parse(utc_time).in_tz('US/Eastern')
    return str(est_time)

@task()
def get_daily_stock_list():
    ts = get_ts()
    dt = ds_add(ts[:10],-1)
    print(f"get_daily_stock is running!!!!!!!!!! logical timestamp is " + ts)
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
    start_time = ts[:10]
    end_time = ds_add(ts[:10], 1)

    postgres_hook = PostgresHook(postgres_conn_id="postgres_localhost", schema='datadl')
    read_sql_df = postgres_hook.get_pandas_df(sql=f'''select exists(select 1
                                                    from {tb_nm} 
                                                    where date_time = '{start_time}') as exists
                                                ''')

    # print('read_sql_df.iloc[0,0] is : ', read_sql_df.iloc[0,0], 'with type ', type(read_sql_df.iloc[0,0]))
    if read_sql_df.iloc[0,0]:
        print(f'database already has record for date {start_time}')
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
        postgres_hook = PostgresHook(postgres_conn_id="postgres_localhost", schema='datadl')
        print("---------------- start writing table to postegresql database ----------------")
        df_all.to_sql(tb_nm, postgres_hook.get_sqlalchemy_engine(), 
                    if_exists='append', index=False, chunksize=5000,
                    method=insert_do_nothing_on_conflicts)
        print("---------------- finished writing table to postegresql database ----------------")
        



@dag(dag_id='day_stock_downloader_v0002', 
     default_args=default_args, 
     start_date=pendulum.datetime(2022, 12, 27, tz="US/Eastern"), 
     schedule_interval='0 22 * * Mon-Fri',
     concurrency=16,
     max_active_runs=16)
def day_price_dl():
    create_daily_postgres_table = PostgresOperator(
            task_id='create_daily_postgres_table',
            postgres_conn_id='postgres_localhost',
            sql="""
                create table if not exists stock_price_daily (
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

@dag(dag_id='minute_stock_downloader_v0003', 
     default_args=default_args, 
     start_date=pendulum.datetime(2023, 1, 1, tz="US/Eastern"), 
     schedule_interval='0 22 * * Mon-Fri',
     concurrency=16,
     max_active_runs=16)
def minute_price_dl():
    create_minute_postgres_table = PostgresOperator(
            task_id='create_minute_postgres_table',
            postgres_conn_id='postgres_localhost',
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