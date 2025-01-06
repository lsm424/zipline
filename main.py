import pickle
from tzlocal import get_localzone
from click.testing import CliRunner
import pandas as pd
import os
from zipline import TradingAlgorithm
from zipline.__main__ import run
from zipline.data.bundles import register, ingest
# from zipline.__main__ import ingest, run
from zipline.api import order_target, record, symbol, set_symbol_lookup_date
from zipline.data.data_portal import DataPortal
from zipline.finance import commission, slippage
from zipline.utils.calendar_utils import get_calendar
from zipline.data.bcolz_minute_bars import BcolzMinuteBarWriter
from zipline.api import order, record, symbol
from zipline.data.bundles.csvdir import csvdir_bundle
from zipline import run_algorithm
from zipline.data import bundles
from zipline.assets import AssetFinder
import time
import loguru
import pandas as pd
import pysnooper
loguru.logger.add("./zipline.log", colorize=True, level="INFO", encoding="utf-8", retention="5 days", rotation="1 day", enqueue=True)
logger = loguru.logger
start = time.time()

# if os.path.exists('dma.pickle'):
#     results = pickle.load(open('dma.pickle', 'r'))
#     results = pd.read_pickle('dma.pickle')
#     logger.info(results.describe())


calendar = get_calendar('XSHG')


register(
    'csvdir',
    csvdir_bundle,
    calendar_name='XSHG',
    minutes_per_day=330,
    # start_session=pd.Timestamp('2023-01-01'),
    # end_session=pd.Timestamp('2023-12-31'),
)
os.environ['QUANDL_API_KEY'] = 'y87uEYuxHxDFW5Mp1zRx'
os.environ["CSVDIR"] = '.'
# result = CliRunner().invoke(ingest, ['--bundle', 'quandl', '--assets-version', '1'])
ingest('csvdir', assets_versions=[1])

# 加载你所用的 bundle 数据
bundle_data = bundles.load('csvdir')  # 根据你使用的 Bundle 调整名称
# 获取 AssetFinder
asset_finder = bundle_data.asset_finder
# 获取所有资产
syms = asset_finder.retrieve_all(range(5384))

data = DataPortal(
    bundle_data.asset_finder,
    trading_calendar=calendar,
    first_trading_day=bundle_data.equity_minute_bar_reader.first_trading_day,
    equity_minute_reader=bundle_data.equity_minute_bar_reader,
    equity_daily_reader=bundle_data.equity_daily_bar_reader,
    adjustment_reader=bundle_data.adjustment_reader,
    future_minute_reader=bundle_data.equity_minute_bar_reader,
    future_daily_reader=bundle_data.equity_daily_bar_reader,
)


def test():
    dt = pd.Timestamp('2024-10-09 09:31:00', tz=get_localzone())
    for sym in syms:
        ret = data.get_spot_value(sym, 'price', dt, 'minute')


def initialize(context):
    context.i = 0
    context.syms = syms
    context.open_time = calendar.open_times[0][1]
    context.close_time = calendar.close_times[0][1]
    context.break_start_time = calendar.break_start_times[0][1]
    context.break_end_time = calendar.break_end_times[0][1]
    context.middle = 5384 // 2


def handle_data(context, data):
    if context.i == 0:
        data.current(context.syms, 'volumn')

    context.i += 1
    cur_time = data.current_dt.time()
    if cur_time < context.open_time or cur_time > context.close_time or context.break_start_time < cur_time <= context.break_end_time:
        return

    start = time.time()
    prices = data.current(context.syms, 'price')
    read_time = time.time()
    prices = prices.sort_values()
    sort_time = time.time()
    sym = prices.index[context.middle]
    median_price = prices[context.middle]
    available_cash = context.portfolio.cash
    shares_to_buy = int(available_cash // median_price)
    if shares_to_buy > 0:
        order_target(sym, shares_to_buy)
    logger.info(f"{data.current_dt} 全仓买入 {shares_to_buy} 股 {sym}，总耗时：{time.time() - start}s，读取耗时：{read_time - start}，排序耗时：{sort_time - read_time}，available_cash：{available_cash}")


# result = CliRunner().invoke(run, ['-f', 'zipline_pro.py', '--trading-calendar', 'XSHG', '--start', '2025-10-09', '--end',
#                                   '2025-12-31', '--data-frequency', 'minute', '--bundle', 'csvdir', '--benchmark-sid', '0', '-o', 'dma.pickle'])
# run_algorithm(
#     start=pd.Timestamp('2024-10-09'),
#     end=pd.Timestamp('2024-12-31'),
#     trading_calendar=calendar,
#     initialize=initialize,
#     handle_data=handle_data,
#     data_frequency='minute',
#     bundle='csvdir',
#     capital_base=10e6,
#     output='dma.pickle',
# )
logger.info(f"耗时：{time.time() - start}s")
