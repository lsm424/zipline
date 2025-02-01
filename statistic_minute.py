# import vaex
import datetime
from tzlocal import get_localzone
import numba
from pandas import Timedelta
import dask.dataframe as dd
from dask.distributed import Client
import pandas as pd
from pandarallel import pandarallel
import sqlite3
import time
from filelock import FileLock
from functools import lru_cache
from sqlalchemy import PrimaryKeyConstraint, UniqueConstraint, create_engine, Column, Integer, String, TIMESTAMP, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import csv
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
import vaex
import os
import multiprocessing
from concurrent.futures import ALL_COMPLETED, ThreadPoolExecutor, ProcessPoolExecutor, FIRST_COMPLETED, wait
import loguru
import swifter
import polars as pl
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

loguru.logger.add("./zipline.log", colorize=True, level="INFO", encoding="utf-8", retention="5 days", rotation="1 day", enqueue=True)
logger = loguru.logger


def decompress_bz2(source_path, file='snap.csv'):
    snap = os.path.dirname(source_path) + f'/{file}'
    if os.path.exists(snap):
        return snap
    destination_path = os.path.dirname(source_path)
    cmd = f'cd {destination_path} && tar xvf {source_path}'
    logger.info(cmd)
    if os.system(cmd):
        logger.info(f'error {source_path}')
        return source_path
    return snap


def find_files(rootdir, name='snap.csv.tar.bz2', start=0, count=0):
    all_files = []
    for root, dirs, files in os.walk(rootdir):
        if name not in files:
            continue
        all_files.append(os.path.join(root, name))
    all_files = sorted(all_files, reverse=False)[start:]
    if count:
        all_files = all_files[:count]
    return all_files


def decompress_dir(rootdir, start=0, count=0, bzfile='snap.csv.tar.bz2', file='snap.csv'):
    all_files = find_files(rootdir, start=start, name=bzfile, count=count)
    executor = ThreadPoolExecutor(max_workers=100)
    r = list(map(lambda x: executor.submit(decompress_bz2, x, file), all_files))
    wait(r, return_when=ALL_COMPLETED)


def _save(df: pd.DataFrame):
    stock = df['stock'].iloc[0]
    df = df.drop('stock', axis=1)
    df.reset_index()
    df.set_index('time', inplace=True)
    file = f'minute/{stock}.csv'
    lock = FileLock(file + '.lock')
    with lock.acquire(timeout=-1):
        header = not os.path.exists(file)
        df.to_csv(file, mode='a', header=header, index=True)


def _statitic_snap(group):
    return pd.DataFrame({
        'high': [group['price'].max()],
        'low': [group['price'].min()],
        'open': [group['price'].loc[group['price'].idxmin()]],
        'close': [group['price'].loc[group['price'].idxmax()]],
        'volume': [group['volume'].loc[group['volume'].idxmax()] - group['volume'].loc[group['volume'].idxmin()]]
    })


def snap_time_proces(df: pd.DataFrame):
    df['real_time'] = df['real_time'].astype(str)
    df = df[df['real_time'].str.len() <= 6]
    df['real_time'] = df['real_time'].str.zfill(6)
    return df


def trade_time_proces(df: pd.DataFrame):
    df['real_time'] = df['time'].astype(str).str.zfill(8)
    # df = df.with_columns(df['time'].cast(pl.Utf8).str.zfill(8))
    # df = df[df['time'].str.len() == 8]
    # df['time'] = pd.to_datetime(date + ' ' + df['time'].str[:2] + ':' + df['time'].str[2:4] + ':' + df['time'].str[4:6] + '.' + df['time'].str[6:9])
    return df


def time_to_seconds(t):
    return t.hour * 3600 + t.minute * 60 + t.second


# 国内股市开市时间 确保 time 列是 datetime 类型
market_open_morning_str = '09:30:00'
market_open_morning = pd.to_datetime(market_open_morning_str).time()
market_open_morning_int = int(pd.to_datetime(market_open_morning_str).timestamp() - pd.to_datetime('00:00:00').timestamp())

market_close_morning_str = '11:30:00'
market_close_morning = pd.to_datetime(market_close_morning_str).time()
market_close_morning_int = int(pd.to_datetime(market_close_morning_str).timestamp() - pd.to_datetime('00:00:00').timestamp())

market_open_afternoon_str = '13:00:00'
market_open_afternoon = pd.to_datetime(market_open_afternoon_str).time()
market_open_afternoon_int = int(pd.to_datetime(market_open_afternoon_str).timestamp() - pd.to_datetime('00:00:00').timestamp())

market_close_afternoon_str = '15:00:00'
market_close_afternoon = pd.to_datetime(market_close_afternoon_str).time()
market_close_afternoon_int = int(pd.to_datetime(market_close_afternoon_str).timestamp() - pd.to_datetime('00:00:00').timestamp())

minutes_per_day = 4 * 60
minutes_per_half_day = minutes_per_day // 2
seconds_per_day = minutes_per_day * 60
seconds_per_half_day = seconds_per_day // 2
interval_seconds = time_to_seconds(market_open_afternoon) - time_to_seconds(market_close_morning)
interval_minutes = interval_seconds // 60
base_minute = pd.Timestamp('1990-01-01 09:30:00')


@lru_cache(maxsize=10000)
def calc_minute(cur_second: pd.Timestamp, base_second: pd.Timestamp) -> pd.Timestamp:
    # 计算start和base之间秒级的tick数
    days = (cur_second - base_second).days
    start_time = cur_second.time()
    ticks = days * seconds_per_day + time_to_seconds(start_time) - time_to_seconds(market_open_morning)
    if start_time > market_open_afternoon:
        ticks -= interval_seconds

    # tick数转换为分钟数，得到分钟级时间
    days = ticks // minutes_per_day
    minutes = ticks % minutes_per_day
    cur_minute = base_minute + Timedelta(days=days, minutes=minutes)
    if minutes > minutes_per_half_day:
        cur_minute += Timedelta(minutes=interval_minutes)
    return cur_minute


config = {
    'snap': {
        'usecols': [0, 1, 2, 8],
        'names': ['time', 'stock', 'price', 'volume'],
        'time_handle': snap_time_proces,
        '_statitic': _statitic_snap,
        'ceil': 'T'  # 按照分钟取整
    },
    # 'trade': {
    #     'usecols': [2, 3, 4, 5],
    #     'names': ['stock', 'time', 'price', 'volume'],
    #     'time_handle': trade_time_proces,
    #     '_statitic': _statitic_trade,
    #     'ceil': 'S'  # 按照秒取整
    # }
}


def analys(file, date=None, data_type='snap', base_second=None):
    pandarallel.initialize(progress_bar=True)  # 启用进度条，并设置4个并行进程
    start = time.time()
    size = os.path.getsize(file)
    if size == 0:
        logger.info(f'finish {file} {time.time() - start}, empty file')
        return None

    logger.info(f'start {file} {size}')
    date = os.path.basename(os.path.dirname(file)) if not date else date
    client = Client()
    df = dd.read_csv(file, usecols=config[data_type]['usecols'], names=config[data_type]['names'])
    df = config[data_type]['time_handle'](df)
    logger.info(f'{file} time handled {df.shape} {df.info(memory_usage="deep")}')
    df['real_time'] = dd.to_datetime(date + df['real_time'].str[:6]) + Timedelta(seconds=1)
    df['time_part'] = df['real_time'].dt.time
    logger.info(f'{file} to datetime')
    df = df[
        ((df['time_part'] > market_open_morning) & (df['time_part'] <= market_close_morning)) |
        ((df['time_part'] > market_open_afternoon) & (df['time_part'] <= market_close_afternoon))
    ]
    df = df.drop('time_part', axis=1)
    # df = df.compute()
    logger.info(f'{file} filter date {df.shape} {df.info(memory_usage="deep")}')
    # 提取分钟，创建一个新的列用于分组（保留整分钟）
    # df['real_time'] = df['time'].map(lambda x: x.ceil(config[data_type]['ceil']))
    df = df.groupby(['stock', 'real_time'], sort=False).apply(config[data_type]['_statitic']).reset_index()
    if 'level_2' in df:
        df = df.drop('level_2', axis=1)
    df['time'] = df['real_time'].map(lambda x: calc_minute(x, base_second), meta=pd.Series([], dtype='datetime64[ns]'))
    df = df.compute()
    df.groupby(['stock'], sort=False).parallel_apply(_save)
    logger.info(f'finish {file} {time.time() - start} {df.shape} {df.info(memory_usage="deep")}')
    return None


def _statitic_trade(group):
    return pd.DataFrame({
        'high': [group['price'].max() / 1000],
        'low': [group['price'].min() / 1000],
        'open': [group['price'].loc[group['time'].idxmax()] / 1000],
        'close': [group['price'].loc[group['time'].idxmin()] / 1000],
        'volume': [group['volume'].sum()]
    })


def analys_swift(file, date=None, base_second=None, idx=0):
    pandarallel.initialize(progress_bar=True)  # 启用进度条，并设置4个并行进程
    start = time.time()
    size = os.path.getsize(file)
    if size == 0:
        logger.info(f'[{idx}]finish {file} {time.time() - start}, empty file')
        return None

    logger.info(f'start {file} {size}')
    date = os.path.basename(os.path.dirname(file)) if not date else date
    columns = [2, 3, 4, 5]
    names = ['stock', 'time', 'price', 'volume']
    df = pl.read_csv(file, has_header=False, columns=columns)
    cols = {f'column_{col_id + 1}': names[idx] for idx, col_id in enumerate(columns)}
    df = df.rename(cols).to_pandas()
    df['real_time'] = df['time'].astype(str).str.zfill(8)
    logger.info(f'[{idx}]{file} time handled {df.shape} {df.info(memory_usage="deep")}')
    df['real_time'] = pd.to_datetime(date + df['real_time'].str[:6]) + Timedelta(seconds=1)
    df['time_part'] = df['real_time'].dt.time
    logger.info(f'[{idx}]{file} to datetime')
    df = df[
        ((df['time_part'] > market_open_morning) & (df['time_part'] <= market_close_morning)) |
        ((df['time_part'] > market_open_afternoon) & (df['time_part'] <= market_close_afternoon))
    ]
    df = df.drop('time_part', axis=1)
    # df = df.compute()
    logger.info(f'[{idx}]{file} filter date {df.shape} {df.info(memory_usage="deep")}')
    # 提取分钟，创建一个新的列用于分组（保留整分钟）
    # df['real_time'] = df['time'].swifter.apply(lambda x: x.ceil(config[data_type]['ceil']))
    client = Client()
    df = dd.from_pandas(df, npartitions=10)
    df = df.groupby(['stock', 'real_time'], sort=False).apply(_statitic_trade).compute().reset_index()
    # df = df.groupby(['stock', 'real_time'], sort=False).apply(config[data_type]['_statitic']).reset_index()
    if 'level_2' in df:
        df = df.drop('level_2', axis=1)
    logger.info(f'[{idx}]{file} statistic {time.time() - start} {df.info(memory_usage="deep")}, client: {client}')
    df['time'] = df['real_time'].swifter.apply(lambda x: calc_minute(x, base_second))
    logger.info(f'{file} gen time {time.time() - start} {df.info(memory_usage="deep")}')
    # df.groupby(['stock'], sort=False).parallel_apply(_save)
    logger.info(f'[{idx}]finish {file} {time.time() - start} {df.shape} {df.info(memory_usage="deep")}')
    client.close()
    return df


def analys_vaxe(file, date=None, base_second=None, idx=0):
    def _to_second(time_int):
        time_str = f"{time_int:08d}"
        # 提取小时、分钟、秒、毫秒
        hours = int(time_str[:2])
        minutes = int(time_str[2:4])
        seconds = int(time_str[4:6])
        # milliseconds = int(time_str[6:])

        # 计算总秒数
        total_seconds = int(hours * 3600 + minutes * 60 + seconds) + 1
        return total_seconds

    pandarallel.initialize(progress_bar=True)  # 启用进度条，并设置4个并行进程
    start = time.time()
    size = os.path.getsize(file)
    if size == 0:
        logger.info(f'[{idx}]finish {file} {time.time() - start}, empty file')
        return None

    logger.info(f'start {file} {size}')
    date = os.path.basename(os.path.dirname(file)) if not date else date
    date_second = datetime.datetime.strptime(date, "%Y%m%d").replace(
        tzinfo=get_localzone()
    ).timestamp()
    columns = [2, 3, 4, 5]
    names = ['stock', 'time', 'price', 'volume']
    df = vaex.read_csv(file, usecols=columns, names=names, delimiter=',', header=None)
    df['time_sec'] = df['time'].apply(_to_second)
    df = df[
        ((df['time_sec'] > market_open_morning_int) & (df['time_sec'] <= market_close_morning_int)) |
        ((df['time_sec'] > market_open_afternoon_int) & (df['time_sec'] <= market_close_afternoon_int))
    ]
    logger.info(f'[{idx}]{file} filter time {df.shape} {time.time() - start}')
    # 提取分钟，创建一个新的列用于分组（保留整分钟）
    df = df.sort(by=['stock', 'time_sec'], ascending=True)
    df = df.groupby(['stock', 'time_sec'], agg={
        'high': vaex.agg.max('price'),         # 最大的value
        'low': vaex.agg.min('price'),         # 最大的value
        'open': vaex.agg.first('price'),  # time最小对应的value
        'close': vaex.agg.last('price'),   # time最大对应的value
        'volume': vaex.agg.sum('volume'),          # value的总和
    })
    df['real_time'] = df['time_sec'].apply(lambda x: x + date_second).astype("datetime64[s]")
    df = df.drop('time_sec')
    df['time'] = df['real_time'].apply(lambda x: calc_minute(x, base_second))
    df['high'] /= 1000
    df['low'] /= 1000
    df['open'] /= 1000
    df['close'] /= 1000
    logger.info(f'[{idx}]{file} statistic groupby {time.time() - start}')
    df.to_pandas_df().groupby(['stock'], sort=False).parallel_apply(_save)
    logger.info(f'[{idx}]finish {file} {time.time() - start} {df.shape}')
    return df


if __name__ == '__main__':
    # os.system('rm minute -rf && mkdir minute')
    start = time.time()
    rootdir = '/data/sse/'
    # logger.info(decompress_dir(rootdir, 0, 500, 'trade.csv.tar.bz2', 'trade.csv'))
    files = find_files(rootdir, 'trade.csv')
    files = sorted(files)
    base_second = pd.Timestamp(min(map(lambda x: os.path.basename(os.path.dirname(x)), files)) + ' 09:30:00')
    # df1 = analys_vaxe('/data/sse/20240430/trade.csv',  base_second=base_second)
    # df2 = analys_swift('/data/sse/20230610/trade.csv',  base_second=base_second)
    # 20230610
    # calc_minute(pd.Timestamp('2023-01-02 14:30:01'))
    # files = list(set(files) - set(not_need))
    # logger.info(f'files: {files}, {len(files)}')
    # files = ['/data/sse/20230610/trade.csv', '/data/sse/20230610/trade.csv']
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = [executor.submit(analys_vaxe, arg, base_second=base_second, idx=idx) for idx, arg in enumerate(files)]
        wait(futures, return_when=ALL_COMPLETED)
