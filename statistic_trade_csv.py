import argparse
import datetime
from exchange_calendars import get_calendar
import numpy as np
from tzlocal import get_localzone
import traceback
# from pandas import Timedelta
# import dask.dataframe as dd
# from dask.distributed import Client
import pandas as pd
from functools import reduce
from pandarallel import pandarallel
import time
from filelock import FileLock
from functools import lru_cache
import vaex
import os
from concurrent.futures import ALL_COMPLETED, ThreadPoolExecutor, ProcessPoolExecutor, FIRST_COMPLETED, wait
import loguru
# import swifter
# import polars as pl
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

# loguru.logger.add("./zipline.log", level="INFO", encoding="utf-8", retention="5 days", rotation="1 day", enqueue=True)
logger = loguru.logger

calendar = get_calendar('XSHG', start=pd.Timestamp("1991-01-01"), side='right')
first_minutes = calendar.first_minutes.apply(lambda x: x.replace(hour=1, minute=31, second=0))
last_am_minutes = calendar.last_am_minutes.apply(lambda x: x.replace(hour=3, minute=30, second=0))
first_pm_minutes = calendar.first_pm_minutes.apply(lambda x: x.replace(hour=5, minute=1, second=0))
last_minutes = calendar.last_minutes.apply(lambda x: x.replace(hour=7, minute=00, second=0))
all_timestamps = pd.to_datetime(np.concatenate([
    np.arange(a.value, b.value + 1, (10**9) * 60)  # 10**9 是 1 秒的纳秒数
    for a, b in zip(first_minutes, last_am_minutes)
] + [
    np.arange(a.value, b.value + 1, (10**9) * 60)  # 10**9 是 1 秒的纳秒数
    for a, b in zip(first_pm_minutes, last_minutes)
]))
all_timestamps = sorted(all_timestamps + pd.Timedelta(hours=8))


def decompress_bz2(source_path, file='snap.csv'):
    snap = os.path.dirname(source_path) + f'/{file}'
    # if os.path.exists(snap):
    #     return snap
    destination_path = os.path.dirname(source_path)
    cmd = f'cd {destination_path} && tar xvf {source_path}'
    if file == 'entrust_data.csv':  # 对于szse数据要进一步处理：8个字段一行的为trade数据，并且把倒数第二行的时间插入到第三列
        cmd += " -O | awk -F, 'NF == 8' |awk -F, '{temp = $7; $7 = $6; $6 = $5; $5 = $4; $4 = temp; print $0 }' OFS=, > trade.csv"

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


def _save(df: pd.DataFrame, resultdir):
    stock = df['stock'].iloc[0]
    df = df.drop('stock', axis=1)
    df.reset_index()
    df.set_index('time', inplace=True)
    file = f'{resultdir}/{stock}.csv'
    lock = FileLock(file + '.lock')
    with lock.acquire(timeout=-1):
        header = not os.path.exists(file)
        df.to_csv(file, mode='a', header=header, index=True)


def snap_time_proces(df: pd.DataFrame):
    df['real_time'] = df['real_time'].astype(str)
    df = df[df['real_time'].str.len() <= 6]
    df['real_time'] = df['real_time'].str.zfill(6)
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
    # cur_second = cur_second.replace(tzinfo=datetime.timezone.utc) + datetime.timedelta(hours=8)
    days = (cur_second - base_second).days
    start_time = cur_second.time()
    ticks = days * seconds_per_day + time_to_seconds(start_time) - time_to_seconds(market_open_morning) - 1
    if start_time > market_open_afternoon:
        ticks -= interval_seconds

    return all_timestamps[ticks]
    # tick数转换为分钟数，得到分钟级时间
    # days = ticks // minutes_per_day
    # minutes = ticks % minutes_per_day
    # cur_minute = base_minute + Timedelta(days=days, minutes=minutes)
    # if minutes > minutes_per_half_day:
    #     cur_minute += Timedelta(minutes=interval_minutes)
    # return cur_minute


config = {
    'snap': {
        'usecols': [0, 1, 2, 8],
        'names': ['time', 'stock', 'price', 'volume'],
        'time_handle': snap_time_proces,
        'ceil': 'T'  # 按照分钟取整
    },
}


def analys_vaxe(file, date=None, base_second=None, idx=0, resultdir='minute', filter_stocks=[]):
    pandarallel.initialize(progress_bar=True)  # 启用进度条，并设置4个并行进程

    def _to_second(time_int):
        time_str = f"{time_int:08d}"
        time_start_pos = 8 if len(time_str) >= 14 else 0
        # 提取小时、分钟、秒、毫秒
        hours = int(time_str[time_start_pos:2 + time_start_pos])
        minutes = int(time_str[time_start_pos + 2: 4 + time_start_pos])
        seconds = int(time_str[time_start_pos + 4: time_start_pos + 6])
        # milliseconds = int(time_str[6:])
        # 计算总秒数
        total_seconds = int(hours * 3600 + minutes * 60 + seconds) + 1
        return total_seconds
    try:
        start = time.time()
        size = os.path.getsize(file)
        if size == 0:
            logger.info(f'[{idx}]finish {file} {time.time() - start}, empty file')
            return None

        logger.info(f'start {file} {size}')
        date = os.path.basename(os.path.dirname(file)) if not date else date
        date_second = int(datetime.datetime.strptime(date, "%Y%m%d").replace(tzinfo=get_localzone()).timestamp())  # 1970.1.1日到date这一天的秒数
        # 读取csv文件
        columns, names = [2, 3, 4, 5], ['stock', 'time', 'price', 'volume']
        df = vaex.read_csv(file, usecols=columns, names=names, delimiter=',', header=None, dtype={'stock': str})
        # 过滤非交易时间的数据
        df['time_sec'] = df['time'].apply(_to_second)
        filter_condition = (((df['time_sec'] > market_open_morning_int) & (df['time_sec'] <= market_close_morning_int)) |
                            ((df['time_sec'] > market_open_afternoon_int) & (df['time_sec'] <= market_close_afternoon_int))) & \
            (df['stock'].str.startswith('00') | df['stock'].str.startswith('60'))
        # 过滤股票
        filter_stock_condition = 1 == 1
        for stock in filter_stocks:
            filter_stock_condition |= df['stock'].str.startswith(stock)
        filter_condition &= filter_stock_condition

        df = df[filter_condition]
        logger.info(f'[{idx}]{file} filter time {df.shape} {time.time() - start}')
        # 统计hlocv
        df = df.sort(by=['stock', 'time_sec'], ascending=True)
        df = df.groupby(['stock', 'time_sec'], agg={
            'high': vaex.agg.max('price'),         # 最大的value
            'low': vaex.agg.min('price'),         # 最大的value
            'open': vaex.agg.first('price'),  # time最小对应的value
            'close': vaex.agg.last('price'),   # time最大对应的value
            'volume': vaex.agg.sum('volume'),          # value的总和
        })
        df['high'] /= 1000
        df['low'] /= 1000
        df['open'] /= 1000
        df['close'] /= 1000
        # 计算真实时间的秒数
        df['real_time'] = df['time_sec'] + date_second
        df = df.drop('time_sec')
        # 映射分钟级时间
        df['time'] = (df['real_time'].astype('int64').astype("datetime64[s]") + (8 * 3600)).apply(lambda x: calc_minute(x, base_second))
        logger.info(f'[{idx}]{file} statistic groupby {time.time() - start} {df.shape}')
        # 分股票写文件
        df = df.to_pandas_df()
        df.groupby(['stock'], sort=False).parallel_apply(lambda x: _save(x, resultdir))
        logger.info(f'[{idx}]finish {file} {time.time() - start} {df.shape}')
        return df['time'].max(), df['time'].min()
    except Exception as e:
        logger.error(f'[{idx}] error {file} {time.time() - start} {e} {traceback.format_exc()}')
        return None


def save(x):
    pd.read_csv(x).sort_values(by='real_time', ascending=True).set_index('time').to_csv(x)


def statistic_trade(start_date, end_date, resultdir, rootdirs, filter_stock=[]):
    logger.info(f'start: {start_date}, end: {end_date}, rootdirs: {rootdirs}')
    start = time.time()
    files = reduce(lambda x, y: x + y, map(lambda x: find_files(x, 'trade.csv'), rootdirs))
    print(start_date, end_date)
    if start_date:
        files = list(filter(lambda x: os.path.basename(os.path.dirname(x)) >= start_date, files))
    if end_date:
        files = list(filter(lambda x: os.path.basename(os.path.dirname(x)) <= end_date, files))
    files = sorted(files, reverse=True)
    start_date = min(map(lambda x: os.path.basename(os.path.dirname(x)), files))
    end_date = max(map(lambda x: os.path.basename(os.path.dirname(x)), files))
    base_second = pd.Timestamp(min(map(lambda x: os.path.basename(os.path.dirname(x)), files)) + ' 09:30:00')
    end_second = pd.Timestamp(max(map(lambda x: os.path.basename(os.path.dirname(x)), files)) + ' 15:00:00')
    date_map = gen_date_map_file(base_second, end_second)

    trade_days = set(date_map['真实时间'].dt.strftime("%Y%m%d"))
    files = list(filter(lambda x: os.path.basename(os.path.dirname(x)) in trade_days, files))
    base_second = pd.Timestamp(min(map(lambda x: os.path.basename(os.path.dirname(x)), files)) + ' 09:30:00')
    end_second = pd.Timestamp(max(map(lambda x: os.path.basename(os.path.dirname(x)), files)) + ' 15:00:00')

    logger.info(f'files: {files}, count: {len(files)}, base_second: {base_second}, filter_stock: {filter_stock}')
    with ProcessPoolExecutor(max_workers=12) as executor:
        os.system(f'rm {resultdir} -rf && mkdir {resultdir}')
        futures = [executor.submit(analys_vaxe, arg, base_second=base_second, idx=idx, resultdir=resultdir, filter_stocks=filter_stock) for idx, arg in enumerate(files)]
        r, _ = wait(futures, return_when=ALL_COMPLETED)
        r = [x for x in [x.result() for x in r] if x]
        r = list(zip(*r))
        max_date, min_date = max(r[0]), min(r[1])

    os.system(f'rm -rf {resultdir}/*.lock')
    logger.info(f'对bar second csv文件重新整理顺序')
    with ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
        all_files = list(map(lambda x: os.path.join(resultdir, x), filter(lambda x: x.endswith('.csv'), os.listdir(resultdir))))
        futures = [executor.submit(save, x) for x in all_files]
        wait(futures, return_when=ALL_COMPLETED)

    logger.info(f'finish {time.time() - start}, max_date: {max_date}, min_date: {min_date}，生成日期映射文件date_map.csv')
    os.system(f'cp {resultdir} {resultdir}_{start_date}_{end_date} -rf')


# 生成日期映射文件
def gen_date_map_file(base_second, end_second):
    import akshare as ak
    start = time.time()
    trade_days = ak.tool_trade_date_hist_sina()['trade_date'].to_list()
    date_map = pd.DataFrame({'真实时间': pd.to_datetime(np.arange(base_second.value + (10**9), end_second.value + (10**9), (10**9) * 1))})
    date_map = date_map[date_map['真实时间'].dt.date.isin(trade_days) &
                        (((date_map['真实时间'].dt.time > market_open_morning) & (date_map['真实时间'].dt.time <= market_close_morning)) |
                         ((date_map['真实时间'].dt.time > market_open_afternoon) & (date_map['真实时间'].dt.time <= market_close_afternoon)))].reset_index(drop=True)
    date_map['映射时间'] = date_map['真实时间'].apply(lambda x: calc_minute(x, base_second))
    date_map['真实时间戳'] = pd.to_datetime(date_map['真实时间']).dt.tz_localize(get_localzone()).apply(lambda x: int(x.timestamp()))
    date_map.set_index('真实时间戳', inplace=True)
    date_map.to_csv('date_map.csv')
    logger.info(f'gen data_map.csv, time use: {time.time() - start}')
    return date_map


if __name__ == '__main__':
    start = time.time()
    parser = argparse.ArgumentParser(description='从trade.csv生成分钟级数据脚本')
    parser.add_argument('--start', type=str, default=None, help='从该起始日期统计trade.csv，仅在type为statistic有效，非必填', required=False)
    parser.add_argument('--end', type=str, default=None, help='统计trade.csv的结束日期，仅在type为statistic有效，非必填', required=False)
    parser.add_argument('--type', type=str, default='statistic', help='操作类型：statistic为从trade.csv统计分钟级数据；decompress为解压出trade.csv。默认statistic', choices=['statistic', 'decompress'])
    parser.add_argument('--rootdir', type=str, default='/data/sse/', help='数据根目录，默认/data/sse/', required=False)
    parser.add_argument('--resultdir', type=str, default='./minute', help='生成的分钟级数据目录，默认./minute', required=False)
    args = parser.parse_args()
    rootdir = args.rootdir.split()
    opr_type = args.type
    opr_type = 'statistic'
    if opr_type == 'decompress':
        logger.info(decompress_dir(rootdir, 0, 500, 'trade.csv.tar.bz2', 'trade.csv'))
        exit(0)
    elif opr_type == 'statistic':
        start_date, end_date = args.start, args.end
        start_date = '20240404'
        end_date = '20240411'
        rootdir = '/data/szse/stock,/data/sse'
        statistic_trade(start_date, end_date, args.resultdir, rootdir.split(','))
