drop table if exists stock_price_minute;


create table if not exists stock (
	ticker character varying(10) not null,
    ts timestamp without time zone not null,
    open_price numeric,
    high_price numeric,
    low_price numeric,
    close_price numeric,
    volume numeric,
    vwap numeric,
    transactions int,
    otc boolean,
    PRIMARY KEY (ticker, ts)
   );
   
  
  
 select * from stock;
 

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

select count(*) from stock_price_daily spd;

select * from stock_price_daily spd where ticker ='AAPL' and date_time='2023-03-01'
order by date_time desc


select date_time,count(*),count(distinct ticker)
from stock_price_daily spd 
group by date_time 
order by date_time

select exists(
	select 1 
	from stock_price_daily 
	where date_time = '2023-01-16'
)

select * from stock_price_daily spd where date_time  = '2023-01-16'
order by date_time desc

select 
substring(to_char(date_time, 'YYYY-MM-DD HH:MI:SS (TZ)') ,1,10) as time_, 
count(*) as cnt_row,
count(distinct ticker) as cnt_ticker
from stock_price_minute spm
group by substring(to_char(date_time, 'YYYY-MM-DD HH:MI:SS (TZ)') ,1,10)
order by time_


select date_time, to_char(date_time, 'YYYY-MM-DD HH:MI:SS (TZ)') 
from stock_price_minute spm 

select ticker, close_price
from stock_price_minute spm 
where date_time >= '2023-04-01' and ticker = 'SPY'


select max_conn,used,res_for_super,max_conn-used-res_for_super res_for_normal 
from 
  (select count(*) used from pg_stat_activity) t1,
  (select setting::int res_for_super from pg_settings where name=$$superuser_reserved_connections$$) t2,
  (select setting::int max_conn from pg_settings where name=$$max_connections$$) t3