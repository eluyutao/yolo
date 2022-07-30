import config
import backtrader, pandas, sqlite3
from datetime import date, datetime, time, timedelta

class OpeningRangeBreakout(backtrader.Strategy):
    params = dict(
        num_opening_bars=15
    )

    def __init__(self):
        self.opening_range_low = 0
        self.opening_range_high = 0
        self.opening_range = 0
        self.bought_today = False
        self.order = None
    
    def log(self, txt, dt=None):
        if dt is None:
            dt = self.datas[0].datetime.datetime()

        print('%s, %s' % (dt, txt))

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            # Buy/Sell order submitted/accepted to/by broker - Nothing to do
            return

        # Check if an order has been completed
        if order.status in [order.Completed]:
            order_details = f"{order.executed.price}, Cost: {order.executed.value}, Comm {order.executed.comm}"

            if order.isbuy():
                self.log(f"BUY EXECUTED, Price: {order_details}")
            else:  # Sell
                self.log(f"SELL EXECUTED, Price: {order_details}")
        
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log('Order Canceled/Margin/Rejected')

        self.order = None

    def notify_trade(self, trade):
        if not trade.isclosed:
            return

        self.log('OPERATION PROFIT, GROSS %.2f, NET %.2f' %
                 (trade.pnl, trade.pnlcomm))

    def next(self):
        current_bar_datetime = self.data.num2date(self.data.datetime[0])
        previous_bar_datetime = self.data.num2date(self.data.datetime[-1])

        if current_bar_datetime.date() != previous_bar_datetime.date():
            self.opening_range_low = self.data.low[0]
            self.opening_range_high = self.data.high[0]
            self.bought_today = False
        
        opening_range_start_time = time(9, 30, 0)
        dt = datetime.combine(date.today(), opening_range_start_time) + timedelta(minutes=self.p.num_opening_bars)
        opening_range_end_time = dt.time()

        if current_bar_datetime.time() >= opening_range_start_time \
            and current_bar_datetime.time() < opening_range_end_time:           
            self.opening_range_high = max(self.data.high[0], self.opening_range_high)
            self.opening_range_low = min(self.data.low[0], self.opening_range_low)
            self.opening_range = self.opening_range_high - self.opening_range_low
        else:
            if self.order:
                return
            
            if self.position and (self.data.close[0] > (self.opening_range_high + self.opening_range)):
                self.close()
                
            if self.data.close[0] > self.opening_range_high and not self.position and not self.bought_today:
                self.order = self.buy()
                self.bought_today = True

            if self.position and (self.data.close[0] < (self.opening_range_high - self.opening_range)):
                self.order = self.close()

            if self.position and current_bar_datetime.time() >= time(15, 45, 0):
                self.log("RUNNING OUT OF TIME - LIQUIDATING POSITION")
                self.close()

    def stop(self):
        self.log('(Num Opening Bars %2d) Ending Value %.2f' %
                 (self.params.num_opening_bars, self.broker.getvalue()))

        if self.broker.getvalue() > 130000:
            self.log("*** BIG WINNER ***")

        if self.broker.getvalue() < 70000:
            self.log("*** MAJOR LOSER ***") 

def run_backtest(db_conn, stock, start_date, end_date, cash=100000.0, opt=False, plot=False):
    print(f"== Testing {stock} ==")

    cerebro = backtrader.Cerebro()
    cerebro.broker.setcash(cash)
    cerebro.addsizer(backtrader.sizers.PercentSizer, percents=95)
    cerebro.broker.setcommission(commission=0.00008)  
    # cerebro.broker.setcommission(commission=1, margin=1)  

    dataframe = pandas.read_sql(f"""
        select datetime, open, high, low, close, volume
        from stock_price_minute
        where symbol = '{stock}'
        and strftime('%H:%M:%S', datetime) >= '09:30:00' 
        and strftime('%H:%M:%S', datetime) < '16:00:00'
        and date(datetime) >= '{start_date}'
        and date(datetime) < '{end_date}'
        order by datetime asc
    """, conn, index_col='datetime', parse_dates=['datetime'])

    data = backtrader.feeds.PandasData(dataname=dataframe)

    cerebro.adddata(data)
    assert not (opt and plot), "Cannot optimize and plot at the same time."
    if opt:
        # strats = cerebro.optstrategy(OpeningRangeBreakout, num_opening_bars=[1, 5, 10, 15, 20, 25, 30, 60])
        strats = cerebro.optstrategy(OpeningRangeBreakout, num_opening_bars=[15])

    else:
        cerebro.addstrategy(OpeningRangeBreakout)

    cerebro.run()
    if plot:
        cerebro.plot()

if __name__ == '__main__':
 conn = sqlite3.connect(config.DB_FILE)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    # print('--- executing sql query ---')
    # cursor.execute("""
    #     SELECT DISTINCT(symbol) as symbol FROM stock_price_minute
    # """)
    # stocks = cursor.fetchall()

    stocks = ['AAPL']
    if training:
        for stock in stocks:
            run_backtest(conn, stock, '2020-07-15', '2021-07-15', opt=False, plot=True)
            break
    else:
        run_backtest(conn, 'AAPL', '2021-07-16', '2022-07-15', opt=False, plot=True)
