create table if not exists stock_price_minute(
    id integer primary key,
    symbol text not null,
    datetime not null,
    open not null,
    high not null,
    low not null,
    close not null,
    volume not null,
    trxn integer
    -- foreign key (stock_id) references stock (id)
)

create table if not exists stock_price_minute_optionable(
    id integer primary key,
    symbol text not null,
    datetime not null,
    open not null,
    high not null,
    low not null,
    close not null,
    volume not null,
    trxn integer
    -- foreign key (stock_id) references stock (id)
)


CREATE unique INDEX idx_stock_minute 
ON stock_price_minute (symbol, datetime);

CREATE unique INDEX idx_stock_minute_optionable 
ON stock_price_minute_optionable (symbol, datetime);

select datetime, open, high, low, close, volume
from stock_price_minute
where symbol = 'AAPL'
and strftime('%H:%M:%S', datetime) >= '09:30:00' 
and strftime('%H:%M:%S', datetime) < '16:00:00'
order by datetime asc


delete from stock_price_minute_optionable