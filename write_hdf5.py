from collections import deque
import argparse
from datetime import datetime, timedelta
from itertools import groupby
import numpy as np
from tzlocal import get_localzone
import traceback
import pandas as pd
import glob
from functools import reduce
from pandarallel import pandarallel
import time
from filelock import FileLock
from functools import lru_cache
import vaex
import os
from concurrent.futures import ALL_COMPLETED, ThreadPoolExecutor, ProcessPoolExecutor, FIRST_COMPLETED, wait
import loguru
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

logger = loguru.logger


def bfs_traverse_directory(root_dir, name):
    # 使用队列进行广度优先遍历
    queue = deque([root_dir])
    all_files = []
    while queue:
        current_dir = queue.popleft()  # 从队列中弹出当前目录
        print(f"当前目录: {current_dir}")

        # 遍历当前目录中的文件和子文件夹
        try:
            for entry in os.scandir(current_dir):
                if entry.is_dir():  # 如果是目录，加入队列
                    queue.append(entry.path)
                elif entry.is_file() and entry.name == name:  # 如果是文件，打印文件名
                    all_files.append(name)
        except PermissionError:
            print(f"没有权限访问: {current_dir}")
            continue
    return all_files


def find_files(rootdir, name='snap.csv.tar.bz2', start=0, count=0):
    # dirs = os.listdir(rootdir)
    # all_files = list(map(lambda x: os.path.join(rootdir, x), filter(lambda x: x == name, dirs)))
    # dirs = list(filter(lambda x: os.path.isdir(os.path.join(rootdir, x)), dirs))
    all_files = []
    for root, dirs, files in os.walk(rootdir):
        if name not in files:
            continue
        all_files.append(os.path.join(root, name))
    all_files = sorted(all_files, reverse=False)[start:]
    if count:
        all_files = all_files[:count]
    return all_files


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


def _save(df: pd.DataFrame, resultdir):
    df['stock'] = list(df['stock'].dropna().unique())[0]
    date = df['date'].iloc[0].replace('-', '')
    df = df.drop('date', axis=1)
    df.reset_index()
    file = f'{resultdir}/{date}.csv'
    lock = FileLock(file + '.lock')
    with lock.acquire(timeout=-1):
        header = not os.path.exists(file)
        df.to_csv(file, mode='a', header=header, index=False)


def analys_vaxe(files: list, idx=0, resultdir='minute', filter_stocks=[], bar_type=1):
    pandarallel.initialize(progress_bar=True)  # 启用进度条，并设置4个并行进程

    def _to_second(time_int):
        time_str = f"{time_int:09d}"
        # 提取小时、分钟、秒、毫秒 计算总秒数
        hours = int(time_str[:2])
        minutes = int(time_str[2: 4])
        seconds = int(time_str[4: 6])
        total_seconds = int(hours * 3600 + minutes * 60 + seconds)
        # total_seconds = datetime.strptime(time_int[:-3], "%Y%m%d%H%M%S").timestamp()
        total_seconds = ((total_seconds // bar_type) + 1) * bar_type
        return total_seconds
    try:
        start = time.time()
        empty_files = list(filter(lambda x: os.path.getsize(x) == 0, files))
        if empty_files:
            logger.info(f'[{idx}]finish empty files {empty_files} {time.time() - start}')
            return None
        files = set(files) - set(empty_files)
        logger.info(f'[{idx}]start files {len(files)}')
        columns, names = [1, 2, 3, 8, 9], ['stock', 'date', 'time', 'price', 'volume']
        df = []

        for file in files:
            date = os.path.basename(os.path.dirname(os.path.dirname(file)))
            d = vaex.read_csv(file, usecols=columns, names=names, delimiter=',', header=None, dtype={'stock': str}, encoding='gb2312', skiprows=1)
            df.append(d)
        df = vaex.concat(df)

        # 过滤非交易时间的数据
        df['time'] = df['time'].apply(_to_second)
        filter_condition = (((df['time'] > market_open_morning_int) & (df['time'] <= market_close_morning_int)) |
                            ((df['time'] > market_open_afternoon_int) & (df['time'] <= market_close_afternoon_int)))
        # 过滤股票
        filter_stock_condition = 1 == 1
        for stock in filter_stocks:
            filter_stock_condition |= df['stock'].str.startswith(stock)
        filter_condition &= filter_stock_condition

        df = df[filter_condition]
        # 统计ohlcv
        df['price'] /= 1000  # sh的价格除以1000，sz的价格除以10000
        df['time'] = df['date'].apply(lambda x: int(datetime.strptime(str(x), "%Y%m%d").timestamp())) + df['time']
        df = df.sort(by=['stock', 'time'], ascending=True)
        df = df.groupby(['stock', 'time'], agg={
            'high': vaex.agg.max('price'),         # 最大的value
            'low': vaex.agg.min('price'),         # 最大的value
            'open': vaex.agg.first('price'),  # time最小对应的value
            'close': vaex.agg.last('price'),   # time最大对应的value
            'volume': vaex.agg.sum('volume'),          # value的总和
        }).to_pandas_df()

        # 补齐缺失的秒级数据
        df['date'] = df['time'].map(lambda x: datetime.fromtimestamp(x).strftime("%Y-%m-%d"))
        full_time_df = []

        def _gen_time(date, time_str: str):
            time_str = (datetime.strptime(f'{date} {time_str}', '%Y-%m-%d %H:%M:%S') + timedelta(seconds=bar_type)).strftime("%Y-%m-%d %H:%M:%S")
            return time_str
        for date in df.date.unique():
            full_time_df += [pd.date_range(start=_gen_time(date, '01:30:00'), end=f'{date} 03:30:00', freq=f'{bar_type}S'),
                             pd.date_range(start=_gen_time(date, '05:00:00'), end=f'{date} 07:00:00', freq=f'{bar_type}S')]
        full_time_df = pd.DataFrame({'time': (np.concatenate(full_time_df).astype('int64') // 1e9).astype('int32')})
        df = full_time_df.merge(df, on='time', how='left').sort_values(by='time', ascending=True)
        df['close'] = df['close'].ffill()
        df['open'] = df['open'].fillna(df['close'])
        df['high'] = df['high'].fillna(df['close'])
        df['low'] = df['low'].fillna(df['close'])
        df['volume'] = df['volume'].fillna(0)
        df['date'] = df['time'].map(lambda x: datetime.fromtimestamp(x).strftime("%Y-%m-%d"))

        # 按天写入csv
        df.groupby(['date'], sort=False).parallel_apply(lambda x: _save(x, resultdir))

        logger.info(f'[{idx}]finish {file} {time.time() - start} {df.shape}')
        return
    except Exception as e:
        logger.error(f'[{idx}] error {file} {time.time() - start} {e} {traceback.format_exc()}')
        return None


def save(x):
    hdf5 = x + '.hdf5'
    if os.path.exists(hdf5):
        return
    logger.info(f'save {x}')
    vaex.read_csv(x, convert=True).sort('stock').export(x)


def statistic_trade(start_date, end_date, resultdir, rootdirs, filter_stock=[], bar_type=1):
    logger.info(f'start: {start_date}, end: {end_date}, rootdirs: {rootdirs}')
    start = time.time()
    files = reduce(lambda x, y: x + y, map(lambda x: find_files(x, '逐笔成交.csv'), rootdirs))
    print(start_date, end_date)
    if start_date:
        files = list(filter(lambda x: os.path.basename(os.path.dirname(os.path.dirname(x))) >= start_date, files))
    if end_date:
        files = list(filter(lambda x: os.path.basename(os.path.dirname(os.path.dirname(x))) <= end_date, files))
    start_date = min(map(lambda x: os.path.basename(os.path.dirname(os.path.dirname(x))), files))
    end_date = max(map(lambda x: os.path.basename(os.path.dirname(os.path.dirname(x))), files))
    if not os.path.exists(resultdir):
        os.makedirs(resultdir)

    import json
    json.dump(files, open('files.json', 'w'))
    files = sorted(files, reverse=True, key=lambda x: os.path.basename(os.path.dirname(x)))  # 按股票代码排序
    files = list(map(lambda x: list(x[1]), groupby(files, key=lambda x: os.path.basename(os.path.dirname(x)))))  # 按股票代码分组
    logger.info(f'files count: {len(files)}, filter_stock: {filter_stock} {time.time() - start}')
    with ProcessPoolExecutor(max_workers=15) as executor:
        os.system(f'rm {resultdir} -rf && mkdir {resultdir}')
        futures = [executor.submit(analys_vaxe, arg, idx=idx, resultdir=resultdir, filter_stocks=filter_stock, bar_type=bar_type) for idx, arg in enumerate(files)]
        wait(futures, return_when=ALL_COMPLETED)

    os.system(f'rm -rf {resultdir}/*.lock')
    logger.info(f'对bar second csv文件重新整理顺序')
    with ProcessPoolExecutor(max_workers=33) as executor:
        all_files = list(map(lambda x: os.path.join(resultdir, x), filter(lambda x: x.endswith('.csv'), os.listdir(resultdir))))
        futures = [executor.submit(save, x) for x in all_files]
        wait(futures, return_when=ALL_COMPLETED)

    logger.info(f'finish {time.time() - start}')
    os.system(f'cp {resultdir} {resultdir}_{start_date}_{end_date} -rf')


if __name__ == '__main__':
    start = time.time()
    parser = argparse.ArgumentParser(description='从trade.csv生成分钟级数据脚本')
    parser.add_argument('--start', type=str, default=None, help='从该起始日期统计"逐笔成交.csv"，仅在type为statistic有效，非必填', required=False)
    parser.add_argument('--end', type=str, default=None, help='统计"逐笔成交.csv"的结束日期，仅在type为statistic有效，非必填', required=False)
    parser.add_argument('--rootdir', type=str, default='/data/share/w2024', help='数据根目录，默认/data/share/w2024', required=False)
    parser.add_argument('--resultdir', type=str, default='./result', help='生成的分钟级数据目录，默认./minute', required=False)
    parser.add_argument('--bar_type', type=int, default=1, help='bar的粒度', required=False, choices=[1, 3, 60])
    args = parser.parse_args()
    args.resultdir = f'{args.resultdir}/{args.bar_type}s_bar'

    # args.resultdir = 'tmp_result'
    # args.rootdir = '/data/transaction/20231226/688778.SH'

    start_date, end_date = args.start, args.end
    statistic_trade(start_date, end_date, args.resultdir, args.rootdir.split(','), bar_type=args.bar_type)
