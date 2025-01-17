# import vaex
import sqlite3
import time
from filelock import FileLock
from datetime import datetime
from functools import reduce
from itertools import groupby
import shutil
from sqlalchemy import PrimaryKeyConstraint, UniqueConstraint, create_engine, Column, Integer, String, TIMESTAMP, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import csv
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
import bz2
import os
import multiprocessing
from concurrent.futures import ALL_COMPLETED, ThreadPoolExecutor, FIRST_COMPLETED, wait
import loguru
import pandas as pd
import dask.dataframe as dd

loguru.logger.add("./zipline.log", colorize=True, level="INFO", encoding="utf-8", retention="5 days", rotation="1 day", enqueue=True)
logger = loguru.logger


def decompress_bz2(source_path):
    snap = os.path.dirname(source_path) + '/snap.csv'
    if os.path.exists(snap):
        return snap
    destination_path = os.path.dirname(source_path)
    cmd = f'cd {destination_path} && tar xvf {source_path}'
    logger.inf(cmd)
    if os.system(cmd):
        logger.info(f'error {source_path}')
        return source_path
    return snap
    destination_path = os.path.splitext(source_path)[0] + '.csv'
    destination_path = os.path.dirname(source_path)
    shutil.unpack_archive(source_path, destination_path, format='bztar')
    # with bz2.open(source_path, 'rb') as source, open(destination_path, 'wb') as destination:
    #     for data in source:
    #         destination.write(data)
    logge.info(f'finish {destination_path}')
    return destination_path


def find_files(rootdir, name='snap.csv.tar.bz2', start=0, count=80):
    all_files = []
    for root, dirs, files in os.walk(rootdir):
        if name not in files:
            continue
        all_files.append(os.path.join(root, name))
    all_files = sorted(all_files, reverse=True)[start:count + start]
    return all_files


def decompress_dir(rootdir, start=0, count=120):
    all_files = find_files(rootdir, start=start, count=count)
    with multiprocessing.Pool(processes=80) as pool:  # 创建含有4个进程的进程池
        all_files = pool.map(decompress_bz2, all_files)
    return all_files


def analys(file, date=None):
    # from pandarallel import pandarallel

    # pandarallel.initialize(progress_bar=True)  # 启用进度条，并设置4个并行进程

    start = time.time()
    logger.info(f'start {file}')
    date = os.path.basename(os.path.dirname(file)) if not date else date
    col_names = ['time', 'stock', 'price', 'm_open_px', 'm_day_high', 'm_day_low', 'm_last_px', 'm_total_trade_number',
                 'volume', 'm_total_value', 'm_total_bid_qty', 'm_total_bid_weighted_avg_px', 'm_total_ask_qty', 'm_total_ask_weighted_avg_Px',
                 'm_bid_depth']
    df = pd.read_csv(file, header=None)
    rows, cols = df.shape
    logger.info(df.shape)
    if cols > len(col_names):
        col_names = col_names + list(map(lambda x: str(x), range(cols - len(col_names))))
    df.columns = col_names
    df['time'] = df['time'].astype(str)
    df = df[df['time'].str.len() <= 6]
    logger.info(f'{file} {df.shape}')
    df['time'] = df['time'].str.zfill(6)
    # 确保 time 列是 datetime 类型
    df['time'] = date[:4] + '-' + date[4:6] + '-' + date[6:8] + ' ' + df['time'].str[:2] + ':' + df['time'].str[2:4] + ':' + df['time'].str[4:6]
    df['time'] = pd.to_datetime(df['time'])
    # 将 time 设置为索引
    df.set_index('time', inplace=True)

    # 提取分钟，创建一个新的列用于分组（保留整分钟）
    df['minute'] = df.index.floor('T')

    def _process(group):
        return pd.DataFrame({
            'high': [group['price'].max()],
            'low': [group['price'].min()],
            'open': [group['price'].loc[group['price'].idxmin()]],
            'close': [group['price'].loc[group['price'].idxmax()]],
            'volume': [group['volume'].loc[group['volume'].idxmax()] - group['volume'].loc[group['volume'].idxmin()]]
        })

    # 按 stock 和 minute 分组
    result = df.groupby(['stock', 'minute']).apply(_process).reset_index()
    logger.info(f'groupby {file} {time.time() - start}')

    def _save(stock, df):
        if 'level_2' in df:
            df = df.drop('level_2', axis=1)
        df.reset_index()
        file = f'minute/{stock[0]}.csv'
        df.set_index('minute', inplace=True)
        lock = FileLock(file + '.lock')
        with lock.acquire(timeout=-1):
            header = not os.path.exists(file)
            df.to_csv(file, mode='a', header=header, index=True)
    executor = ThreadPoolExecutor(max_workers=100)
    r = []
    for stock, df in result.groupby(['stock']):
        r.append(executor.submit(_save, stock, df))
    wait(r, return_when=ALL_COMPLETED)
    logger.info(f'finish {file} {time.time() - start}')


def analys_dd(file, date=None):
    start = time.time()
    if os.path.getsize(file) == 0:
        logger.info(f'finish {file} {time.time() - start}, empty file')
        return None
    from pandarallel import pandarallel

    pandarallel.initialize(progress_bar=True, nb_workers=5)  # 启用进度条，并设置4个并行进程

    logger.info(f'start {file}')
    date = os.path.basename(os.path.dirname(file)) if not date else date
    col_names = ['time', 'stock', 'price', 'm_open_px', 'm_day_high', 'm_day_low', 'm_last_px', 'm_total_trade_number',
                 'volume', 'm_total_value', 'm_total_bid_qty', 'm_total_bid_weighted_avg_px', 'm_total_ask_qty', 'm_total_ask_weighted_avg_Px',
                 'm_bid_depth']
    usecols = [0, 1, 2, 8]
    # df = dd.read_csv(file, header=None).compute()
    df = dd.read_csv(file, usecols=usecols, names=['time', 'stock', 'price', 'volume']).compute()
    # rows, cols = df.shape
    # logger.info(df.shape)
    # if cols > len(col_names):
    #     col_names = col_names + list(map(lambda x: str(x), range(cols - len(col_names))))
    # df.columns = col_names
    df['time'] = df['time'].astype(str)
    df = df[df['time'].str.len() <= 6]
    logger.info(f'{file} {df.shape}')
    df['time'] = df['time'].str.zfill(6)
    # 确保 time 列是 datetime 类型
    market_open_morning = pd.to_datetime('09:30').time()
    market_close_morning = pd.to_datetime('11:30').time()
    market_open_afternoon = pd.to_datetime('13:00').time()
    market_close_afternoon = pd.to_datetime('15:00').time()
    df['time'] = dd.to_datetime(date[:4] + '-' + date[4:6] + '-' + date[6:8] + ' ' + df['time'].str[:2] + ':' + df['time'].str[2:4] + ':' + df['time'].str[4:6])
    df = df[
        ((df['time'].dt.time >= market_open_morning) & (df['time'].dt.time <= market_close_morning)) |
        ((df['time'].dt.time >= market_open_afternoon) & (df['time'].dt.time <= market_close_afternoon))
    ]
    logger.info(f'{file} {df.shape}')

    # df['time'] = df['time'].map(lambda x: pd.to_datetime(x))
    # 将 time 设置为索引
    # df.set_index('time', inplace=True)

    # 提取分钟，创建一个新的列用于分组（保留整分钟）
    df['minute'] = df['time'].parallel_map(lambda x: x.floor('T'))

    def _process(group):
        return pd.DataFrame({
            'high': [group['price'].max()],
            'low': [group['price'].min()],
            'open': [group['price'].loc[group['price'].idxmin()]],
            'close': [group['price'].loc[group['price'].idxmax()]],
            'volume': [group['volume'].loc[group['volume'].idxmax()] - group['volume'].loc[group['volume'].idxmin()]]
        })

    # 按 stock 和 minute 分组
    result = df.groupby(['stock', 'minute'], sort=False).parallel_apply(_process).reset_index()
    logger.info(f'groupby {file} {time.time() - start}')

    def _save(df):
        stock = df['stock'].iloc[0]
        if 'level_2' in df:
            df = df.drop('level_2', axis=1)
        df.reset_index()
        file = f'minute/{stock}.csv'
        df.set_index('minute', inplace=True)
        lock = FileLock(file + '.lock')
        with lock.acquire(timeout=-1):
            header = not os.path.exists(file)
            df.to_csv(file, mode='a', header=header, index=True)

    def _save2(stock, df):
        if 'level_2' in df:
            df = df.drop('level_2', axis=1)
        df.reset_index()
        file = f'minute/{stock[0]}.csv'
        df.set_index('minute', inplace=True)
        lock = FileLock(file + '.lock')
        with lock.acquire(timeout=-1):
            header = not os.path.exists(file)
            df.to_csv(file, mode='a', header=header, index=True)

    result.groupby(['stock'], sort=False).parallel_apply(_save)

    # executor = ThreadPoolExecutor(max_workers=100)
    # r = []
    # for stock, df in result.groupby(['stock']):
    #     r.append(executor.submit(_save, stock, df))
    # wait(r, return_when=ALL_COMPLETED)
    logger.info(f'finish {file} {time.time() - start}')
    return None


if __name__ == '__main__':
    start = time.time()
    rootdir = '/data/sse/'
    # logge.info(decompress_dir(rootdir))
    files = find_files(rootdir, 'snap.csv', count=90)

    # files = list(set(files) - set(not_need))
    logger.info(f'files: {files}, {len(files)}')
    # files = ['/media/USBDISK/sse/20240413/snap.csv']
    # analys_dd(files[0])
    from concurrent.futures import ProcessPoolExecutor
    from multiprocessing import get_context

    # 获取一个进程上下文，这里使用 'fork' 来避免守护进程问题
    ctx = get_context('fork')
    with ProcessPoolExecutor(max_workers=5, mp_context=ctx) as executor:
        futures = [executor.submit(analys_dd, arg) for arg in files]
        results = [f.result() for f in futures]
    logger.info(f'finish {len(results)}  {time.time() - start}')
    # pool = Pool(processes=5)
    # pool.apply_async(func=analys, args=(files[0], None))
    # with Pool(processes=5) as pool:  # 创建含有4个进程的进程池
    #     pool.apply_async(func=analys, files)
    # analys(f'/media/USBDISK/sse/20240430/snap.csv')
    # all_files = find_files(rootdir, 'snap.csv')
    # insert_data(f'/media/USBDISK/sse/20240430/snap.csv')
    # insert_data('20240630', 'snap.csv')
