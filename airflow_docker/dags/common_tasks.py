from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
import pendulum

from airflow.macros import ds_add

try:
    from data_downloader.utils import insert_do_nothing_on_conflicts
    from data_downloader.list_tickers import get_ticker_for_day, get_ticker_price
except:
    from utils import insert_do_nothing_on_conflicts
    from list_tickers import get_ticker_for_day, get_ticker_price

from pandas.tseries.holiday import USFederalHolidayCalendar

from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd

from tqdm import tqdm


DB_CONN_ID = 'TimescaleDB_win_remote'

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
        

if __name__ == "__main__":
    # greet_dag = daily_etl()
    # greet_dag.test(pendulum.datetime(2023, 4, 24, 22, 0, 0, tz="US/Eastern"))
    pass