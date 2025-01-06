import pickle
from typing import Literal
import numpy as np
from tzlocal import get_localzone
from click.testing import CliRunner
import pandas as pd
import os
from exchange_calendars.calendar_helpers import Minute, parse_timestamp
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
import timeit
# from pyinstrument import Profiler
from line_profiler import profile
from zipline.assets import (
    Asset,
    AssetConvertible,
    Equity,
    Future,
    PricingDataAssociable,
)

BASE_FIELDS = frozenset(
    [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "price",
        "contract",
        "sid",
        "last_traded",
    ]
)
loguru.logger.add("./zipline.log", colorize=True, level="INFO", encoding="utf-8", retention="5 days", rotation="1 day", enqueue=True)
logger = loguru.logger


calendar = get_calendar('XSHG', start='2005-01-05')

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

# 加载你所用的 bundle 数据
bundle_data = bundles.load('csvdir')  # 根据你使用的 Bundle 调整名称
# 获取 AssetFinder
asset_finder = bundle_data.asset_finder
# 获取所有资产
syms = asset_finder.retrieve_all(range(5384))

data = DataPortal(
    bundle_data.asset_finder,
    trading_calendar=bundle_data.equity_minute_bar_reader.trading_calendar,
    first_trading_day=bundle_data.equity_minute_bar_reader.first_trading_day,
    equity_minute_reader=bundle_data.equity_minute_bar_reader,
    equity_daily_reader=bundle_data.equity_daily_bar_reader,
    adjustment_reader=bundle_data.adjustment_reader,
    future_minute_reader=bundle_data.equity_minute_bar_reader,
    future_daily_reader=bundle_data.equity_daily_bar_reader,
)


@profile
def minute_to_session(self, minute: Minute, direction: Literal["next", "previous", "none"] = "next", _parse: bool = True) -> pd.Timestamp:
    if _parse:
        minute = parse_timestamp(minute, calendar=self)

    if minute.value < self.minutes_nanos[0]:
        # Resolve call here.
        if direction == "next":
            return self.first_session
        else:
            raise ValueError(
                f"Received `minute` as '{minute}' although this is earlier than the"
                f" calendar's first trading minute ({self.first_minute}). Consider"
                " passing `direction` as 'next' to get first session."
            )

    if minute.value > self.minutes_nanos[-1]:
        # Resolve call here.
        if direction == "previous":
            return self.last_session
        else:
            raise ValueError(
                f"Received `minute` as '{minute}' although this is later than the"
                f" calendar's last trading minute ({self.last_minute}). Consider"
                " passing `direction` as 'previous' to get last session."
            )

    idx = np.searchsorted(self.last_minutes_nanos, minute.value)
    current_or_next_session = self.schedule.index[idx]

    if direction == "next":
        return current_or_next_session
    elif direction == "previous":
        if not self.is_open_on_minute(minute, ignore_breaks=True, _parse=False):
            return self.schedule.index[idx - 1]
    elif direction == "none":
        if not self.is_open_on_minute(minute, ignore_breaks=True, _parse=False):
            # if the exchange is closed, blow up
            raise ValueError(
                f"`minute` '{minute}' is not a trading minute. Consider passing"
                " `direction` as 'next' or 'previous'."
            )
    else:
        # invalid direction
        raise ValueError(f"Invalid direction parameter: {direction}")

    return current_or_next_session


@profile
def _get_single_asset_value(self: DataPortal, session_label: pd.Timestamp, asset: Equity, field: str, dt: pd.Timestamp, data_frequency: str):
    if self._is_extra_source(asset, field, self._augmented_sources_map):
        return self._get_fetcher_value(asset, field, dt)

    if field not in BASE_FIELDS:
        raise KeyError("Invalid column: " + str(field))

    if (
        dt < asset.start_date.tz_localize(dt.tzinfo)
        or (data_frequency == "daily" and session_label > asset.end_date)
        or (data_frequency == "minute" and session_label > asset.end_date)
    ):
        if field == "volume":
            return 0
        elif field == "contract":
            return None
        elif field != "last_traded":
            return np.nan

    if data_frequency == "daily":
        if field == "contract":
            return self._get_current_contract(asset, session_label)
        else:
            return self._get_daily_spot_value(
                asset,
                field,
                session_label,
            )
    else:
        if field == "last_traded":
            return self.get_last_traded_dt(asset, dt, "minute")
        elif field == "price":
            return self._get_minute_spot_value(
                asset,
                "close",
                dt,
                ffill=True,
            )
        elif field == "contract":
            return self._get_current_contract(asset, dt)
        else:
            return self._get_minute_spot_value(asset, field, dt)


@profile
def get_spot_value(self: DataPortal, assets: Equity, field: str, dt: pd.Timestamp, data_frequency: str):
    session_label = self.trading_calendar.minute_to_session(dt)
    ret = data._get_single_asset_value(session_label, assets, field, dt, data_frequency)
    return ret


dt = pd.Timestamp('2024-10-09 09:31:00', tz=get_localzone())
# 初始化，加载bcolz到缓存
list(map(lambda x: data.get_spot_value(x, 'price', dt, 'minute'), syms))

# 测试代码
start = time.time()
[[get_spot_value(data, x, 'price', dt, 'minute') for x in syms] for i in range(10)]
logger.info(f"test_get_spot_value 耗时：{time.time() - start}s")

start = time.time()
[[minute_to_session(data.trading_calendar, dt) for x in syms] for i in range(10)]
logger.info(f"test_min_session 耗时：{time.time() - start}s")

session_label = pd.Timestamp('2024-10-09 00:00:00')
start = time.time()
[[_get_single_asset_value(data, session_label, x, 'price', dt, 'minute') for x in syms] for i in range(10)]
logger.info(f"test_get_single_asset_value 耗时：{time.time() - start}s")

session_label = pd.Timestamp('2024-10-09 00:00:00')
start = time.time()
[[pd.Timestamp('2024-10-09 00:00:00').tz_localize(get_localzone()) for x in syms] for i in range(10)]
logger.info(f"tz_localize 耗时：{time.time() - start}s")

# start = time.time()
# [test_get_minute_spot_value() for i in range(20)]
# logger.info(f"test_get_minute_spot_value 耗时：{time.time() - start}s")


'''
1、pyintruments profiling

start = time.time()
profiler = Profiler(interval=0.0001)
profiler.start()
[test() for i in range(10)]
profiler.stop()
profiler.print()

5.881 <module>  profile_test.py:1
└─ 5.881 <listcomp>  profile_test.py:79
   └─ 5.881 test  profile_test.py:66
      └─ 5.879 <lambda>  profile_test.py:67
         └─ 5.878 DataPortal.get_spot_value  zipline/data/data_portal.py:455
            ├─ 5.699 DataPortal._get_single_asset_value  zipline/data/data_portal.py:412
            └─ 0.170 XSHGExchangeCalendar.minute_to_session  exchange_calendars/exchange_calendar.py:1690
               └─ 0.139 DatetimeIndex.__getitem__  pandas/core/indexes/base.py:5159
                  └─ 0.138 DatetimeArray.__getitem__  pandas/core/arrays/datetimelike.py:359
                     └─ 0.136 DatetimeArray.__getitem__  pandas/core/arrays/_mixins.py:266
                        └─ 0.136 DatetimeArray._box_func  pandas/core/arrays/datetimes.py:527
                           └─ 0.135 Timestamp._from_value_and_reso  <built-in>

2、profile裝飾器
kernprof -lv profile_test.py 

Timer unit: 1e-06 s
Total time: 4.61355 s
File: profile_test.py
Function: test at line 66

Line #      Hits         Time  Per Hit   % Time  Line Contents
==============================================================
    66                                           @profile
    67                                           def test():
    68        10    4613548.6 461354.9    100.0      list(map(lambda x: data.get_spot_value(x, 'price', dt, 'minute'), syms))
    69                                               # for sym in syms:
    70                                               #     ret = data.get_spot_value(sym, 'price', dt, 'minute')

3、cprofile

python3 -m cProfile -o test.profile profile_test.py
snakeviz test.profile

'''
