from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
import itertools
from multiprocessing import get_context
import akshare as ak
import pandas as pd
import numpy as np


def gen_stock(stock, date):
    rng = np.random.default_rng()
    # 定义交易时间段
    trading_hours_morning_df = pd.DataFrame(pd.date_range(f'{date} 09:31', f'{date} 11:30', freq='T'))
    trading_hours_afternoon_df = pd.DataFrame(pd.date_range(f'{date} 13:01', f'{date} 15:00', freq='T'))
    # 合并两个时间段
    trading_hours_df = pd.concat([trading_hours_morning_df, trading_hours_afternoon_df])
    # 生成假数据
    data = {
        'open': rng.uniform(3, 20, size=len(trading_hours_df)).round(2),
        'high': rng.uniform(3, 20, size=len(trading_hours_df)).round(2) + rng.uniform(0, 2, size=len(trading_hours_df)).round(2),
        'low': rng.uniform(3, 20, size=len(trading_hours_df)).round(2) - rng.uniform(0, 2, size=len(trading_hours_df)).round(2),
        'close': rng.uniform(3, 20, size=len(trading_hours_df)).round(2),
        'volume': rng.integers(100, 1000, size=len(trading_hours_df))
    }

    # 创建DataFrame
    df = pd.DataFrame(data, index=trading_hours_df[0].to_list())

    # 确保high是最高的，low是最低的
    df['high'] = df[['open', 'high', 'close']].max(axis=1)
    df['low'] = df[['open', 'low', 'close']].min(axis=1)

    # 将股票代码添加到DataFrame
    df['stock_code'] = stock

    return df


def gen_data(stock, recent_60_days):
    # df = ak.stock_zh_a_minute(symbol=stock, period="1", adjust="qfq", start_date=recent_60_days[0], end_date=recent_60_days[-1])
    data = list(map(lambda x: gen_stock(stock, x), recent_60_days))
    data = pd.concat(data)
    data.to_csv(f'minute/{stock}.csv')
    print(f'{stock} done')


if __name__ == '__main__':
    # 生成最近60天的日期
    trade_days = ak.tool_trade_date_hist_sina()['trade_date'].to_list()
    now = '2025-01-01'
    recent_60_days = map(lambda x: x.strftime('%Y-%m-%d'), trade_days)
    recent_60_days = sorted(filter(lambda x: x < now, recent_60_days), reverse=True)[:60 * 30]
    stocks = ak.stock_info_a_code_name()['code'].to_list()

    with ProcessPoolExecutor(max_workers=100, mp_context=get_context('fork')) as executor:
        futures = [executor.submit(gen_data, arg, recent_60_days) for arg in stocks]
        results = [f.result() for f in futures]
