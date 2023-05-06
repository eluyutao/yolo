from datetime import datetime
import pytz
import sys
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import table, column
import pendulum

EST = pytz.timezone('US/Eastern')
UTC = pytz.utc
FMT = '%Y-%m-%d %H:%M:%S %Z%z'



def insert_do_nothing_on_conflicts(sqltable, conn, keys, data_iter):
    """
    Execute SQL statement inserting data

    Parameters
    ----------
    sqltable : pandas.io.sql.SQLTable
    conn : sqlalchemy.engine.Engine or sqlalchemy.engine.Connection
    keys : list of str
        Column names
    data_iter : Iterable that iterates the values to be inserted
    """

    columns=[]
    for c in keys:
        columns.append(column(c))

    if sqltable.schema:
        table_name = '{}.{}'.format(sqltable.schema, sqltable.name)
    else:
        table_name = sqltable.name

    mytable = table(table_name, *columns)

    insert_stmt = insert(mytable).values(list(data_iter))
    do_nothing_stmt = insert_stmt.on_conflict_do_nothing(index_elements=['ticker', 'date_time'])

    conn.execute(do_nothing_stmt)

def utc_unix_milli_2_est_ts(unix):
    unix = int(unix)
    date = datetime.fromtimestamp(unix/1000, UTC)
    return date.astimezone(EST)

# this function has some issue
def est_ts_2_utc_unix_milli(ts):

    # print('inside est_ts_2_utc_unix_milli, ts = ', ts)
    if type(ts) == str:
        if len(ts) == 19:
            ts = pendulum.parse(ts, tz='US/Eastern')
        elif len(ts) == 10:
            ts = pendulum.parse(ts, tz='US/Eastern')
            # print('step2 ---------', ts)
        else:
            raise ValueError('Wrong input format: expected a string with length 10 or 19.')
    utc_ts = ts.in_tz('UTC')
    # print('step 3 --------', utc_ts)
    r = int(datetime.timestamp(utc_ts)*1000)
    # print('step 4 ---------', r)
    return r


if __name__ == '__main__':
    a = est_ts_2_utc_unix_milli(ts='2023-04-24')
    print(a)
