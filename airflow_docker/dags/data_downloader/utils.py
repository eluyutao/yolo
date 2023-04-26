from datetime import datetime
import pytz

EST = pytz.timezone('US/Eastern')
UTC = pytz.utc
FMT = '%Y-%m-%d %H:%M:%S %Z%z'


def utc_unix_milli_2_est_ts(unix):
    unix = int(unix)
    date = datetime.fromtimestamp(unix/1000, UTC)
    return date.astimezone(EST)

def est_ts_2_utc_unix_milli(ts):
    if type(ts) == str:
        if len(ts) == 19:
            ts = datetime.strptime(ts, '%Y-%m-%d %H:%M:%S')
        elif len(ts) == 10:
            ts = datetime.strptime(ts, '%Y-%m-%d')
    utc_ts = ts.astimezone(UTC)
    return int(datetime.timestamp(utc_ts)*1000)


if __name__ == '__main__':
    a = est_ts_2_utc_unix_milli(ts='2022-07-03 06:02:34')
