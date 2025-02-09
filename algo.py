# encoding:utf-8
import math
from loguru import logger
import numpy as np
from zipline.data import bundles
from zipline.assets import AssetFinder
from tzlocal import get_localzone
import pandas as pd
from zipline import TradingAlgorithm
from zipline.__main__ import run
from zipline.api import order_target, order_target_percent, record, symbol, set_symbol_lookup_date
from zipline.data.data_portal import DataPortal
from zipline.utils.calendar_utils import get_calendar
from zipline.data.bcolz_minute_bars import BcolzMinuteBarWriter
from zipline.api import order, record, symbol
import time
import os
import akshare as ak
from concurrent.futures import ALL_COMPLETED, ThreadPoolExecutor, FIRST_COMPLETED, wait
from datetime import datetime, timedelta

if os.environ.get('ZIPLINE_ROOT') is None:
    os.environ['ZIPLINE_ROOT'] = '/data/zipline'

# stock_file = 'stock_history.csv'
# if os.path.exists(stock_file):
#     stocks = pd.read_csv(stock_file)
# else:
#     stocks = ak.stock_zh_a_spot()
#     stocks.to_csv(stock_file)
# 日历组件
calendar = get_calendar('XSHG')
# 加载你所用的 bundle 数据
bundle_data = bundles.load('csvdir')  # 根据你使用的 Bundle 调整名称
# 获取 AssetFinder
asset_finder = bundle_data.asset_finder
# 获取所有资产对象
syms = list(filter(lambda x: x, asset_finder.retrieve_all(asset_finder.sids, True)))
# 记录所有资产状态
records = pd.DataFrame(
    {
        'stock': list(map(lambda x: x.symbol, syms)),  # 股票代码
        'start_date': list(map(lambda x: x.start_date, syms)),  # 开始日期
        'end_date': list(map(lambda x: x.end_date, syms)),  # 结束日期
        'syms': syms,
    }
)
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
# 股票交易天级数
stock_daliy_dir = 'stock_daliy_csv'
if not os.path.exists(stock_daliy_dir):
    os.makedirs(stock_daliy_dir)


def get_stock_trade_date(stock):
    filename = os.path.join(stock_daliy_dir, stock + '.csv')
    if os.path.exists(filename):
        try:
            df = pd.read_csv(filename)
            return df
        except BaseException as e:
            logger.error(f'读取{filename}失败，原因：{e}， 内容：{open(filename).read()}')

    try:
        full_name = stock
        if not full_name.startswith('sh'):
            full_name = 'sh' + full_name
        df = ak.stock_zh_a_daily(full_name)
        logger.info(f'拉取{stock}数据')
        df['stock'] = stock
    except BaseException as e:
        logger.error(f'拉取{stock}数据失败，原因：{e}')
        df = pd.DataFrame({})
    df.to_csv(filename)
    return df
#######################################################################
# 回测算法
#######################################################################


def initialize(context):
    context.open_time = calendar.open_times[0][1]
    context.close_time = calendar.close_times[0][1]
    context.break_start_time = calendar.break_start_times[0][1]
    context.break_end_time = calendar.break_end_times[0][1]
    context.i = 0  # 帧数统计
    # 筛选6或0开头代码
    context.records = records[records['stock'].str.startswith(('6', '0'))]

    # 上述股票的拉取历史交易数据，用于计算涨停值
    logger.info(f'开始批量拉取{len(context.records)}只股票的历史交易数据')
    executor = ThreadPoolExecutor(max_workers=50)
    r = list(map(lambda x: executor.submit(get_stock_trade_date, x), context.records['stock']))
    wait(r, return_when=ALL_COMPLETED)
    context.stocks = pd.concat([x.result() for x in r])
    context.stocks['stock'] = context.stocks['stock'].astype(int).astype(str)
    context.stocks['date'] = pd.to_datetime(context.stocks['date'])

    # 上面拉的股票数可能有限，进一步过滤股票
    stocks = set(context.stocks['stock']) & set(context.records['stock'])
    context.stocks = context.stocks[context.stocks['stock'].isin(stocks)][['stock', 'date', 'close']]
    context.records = context.records[context.records['stock'].isin(stocks)]
    context.syms = context.records['syms'].to_list()
    context.cur_date = None

    # 初始化
    context.t = 1  # 条件1：超过涨停价的秒数配置
    context.tm = datetime(year=1970, month=1, day=1, hour=14, minute=30, second=30).time()  # 条件2：第一次超过涨停价的时间
    context.pre_n = 10  # 条件3：第二次涨停瞬间的tick的交易量位于当日集合竞价到第二次涨停瞬间所有tick量的前N位的配置
    context.w = 0.01  # 配置买入的权重百分比
    context.default_date = datetime(year=1970, month=1, day=1, hour=0, minute=0, second=0)
    context.records['count'] = 0  # 统计第一次涨停的时长
    context.records['first_time'] = [context.default_date] * len(stocks)   # 统计第一次涨停时间
    context.records['volume_list'] = [[]] * len(stocks)  # 标记生命周期是否结果
    context.records['stage'] = 0  # 标记阶段：0 等待涨停；1 第一次涨停中； 2 第一次涨停结束；3 第二次涨停；4 已买入；5 已卖出/已结束
    logger.info(f'开始回测{len(stocks)}只股票')


def handle_data(context, data):
    cur_time = data.current_dt.time()
    if cur_time < context.open_time or cur_time > context.close_time or context.break_start_time < cur_time <= context.break_end_time:
        return

    context.i += 1
    start = time.time()
    # 当前帧股价
    prices = data.current(context.syms, 'price')
    prices = prices[prices.notnull()]
    if prices.empty:  # 当前帧所有股票数据为空
        logger.warning(f"[{context.i}]{data.current_dt}，所有股票价格为空，跳过")
        return

    # 获得真实时间
    real_time = datetime.fromtimestamp(data.current(prices.index[0], 'real_time'))

    # 如果是新的一天，则更新records中的涨停价、跌停价，以及其他字段初始化
    cur_date = real_time.date()
    if context.cur_date != cur_date:
        context.cur_date = cur_date
        date = pd.to_datetime(cur_date.strftime("%Y-%m-%d"))

        def get_max_date_row(group):
            # 对一个股，筛选出小于给定日期的所有行，并返回最大日期的行
            filtered_group = group[group['date'] < date]
            if not filtered_group.empty:
                return filtered_group.loc[filtered_group['date'].idxmax()]
            return None
        # 筛选每个股票的上一个交易日的数据
        stocks = context.stocks.groupby('stock').apply(get_max_date_row).dropna()[['date', 'close']].reset_index()
        # 得到今天的涨停价、跌停价阈值
        records = context.records.merge(stocks[['stock', 'close']], how='left', on='stock')
        records['limit_up_price'] = records['close'] * 1.1  # 涨停价
        records['limit_down_price'] = records['close'] * 0.9  # 跌停价
        records.loc[records['stage'] != 4, 'count'] = 0
        records.loc[records['stage'] != 4, 'first_time'] = context.default_date
        records.loc[records['stage'] != 4, 'stage'] = 0
        records.loc[records['stage'] != 4, 'volume_list'] = records.loc[records['stage'] != 4, 'volume_list'].apply(lambda x: [])
        context.records = records.drop('close', axis=1)

    # 按stock名称，统计交易量，价格合并到records
    prices = prices.reset_index()
    prices['stock'] = prices['index'].map(lambda x: x.symbol)
    smys = context.records.loc[(context.records['stage'] <= 3) & context.records['stock'].isin(prices['stock'])]['syms'].to_list()
    volume = data.current(smys, 'volume').reset_index()
    volume['stock'] = volume['index'].map(lambda x: x.symbol)
    records = context.records.merge(prices[['stock', 'price']], on='stock', how='left').merge(volume[['stock', 'volume']], on='stock', how='left')
    time_stampe1 = time.time()

    def keep_n_volume(row):  # 将volume插入到有序的volume_list中，并保持前pre_n个值
        if pd.notna(row['volume']):
            insert_pos = np.searchsorted(row['volume_list'], row['volume'])
            if insert_pos < context.pre_n:
                row['volume_list'] = np.insert(row['volume_list'], insert_pos, row['volume']).tolist()
        return row['volume_list']
    records['volume_list'] = records.apply(keep_n_volume, axis=1)
    time_stampe2 = time.time()

    # 超过涨停价的数据索引
    mask_ge = records['price'] >= records['limit_up_price']
    # 之前是未涨停的阶段，则 stage 加1
    records.loc[mask_ge & (records['stage'].isin([0, 2])), 'stage'] += 1
    # 第一次涨停时间为空则设置为当前时间
    records.loc[mask_ge & (records['first_time'] == context.default_date), 'first_time'] = real_time
    # 第一次的连续涨停次数加1
    records.loc[mask_ge & records['stage'] == 1, 'count'] += 1

    # 未超过涨停价的数据索引
    mask_lt = records['price'] < records['limit_up_price']
    # 之前是涨停的阶段，则 stage 加1
    records.loc[mask_lt & (records['stage'] == 1), 'stage'] += 1

    # 若为第二天，若盘中任意时间跌停，则按跌停价卖出，或10点整或14:57分不涨停，则按最新价卖出
    need_sell1 = (records['stage'] == 4) & (records['first_time'].dt.date == (real_time - timedelta(days=1)).date()) & ((records['price'] <= records['limit_down_price']) |
                                                                                                                        ((real_time.hour == 10) & (real_time.minute == 0) & (real_time.second == 0) & (records['price'] < records['limit_up_price'])) |
                                                                                                                        ((real_time.hour == 14) & (real_time.minute == 57) & (real_time.second == 0) & (records['price'] < records['limit_up_price'])))
    # 若为第三天及以后，盘中任意时间跌停，则按跌停价卖出，或14:57分不涨停，则按最新价卖出
    need_sell2 = (records['stage'] == 4) & (records['first_time'].dt.date < (real_time - timedelta(days=1)).date()) & ((records['price'] <= records['limit_down_price']) |
                                                                                                                       ((real_time.hour == 14) & (real_time.minute == 57) & (real_time.second == 0) & (records['price'] < records['limit_up_price'])))
    need_sell = need_sell1 | need_sell2
    if need_sell.any():  # 售出操作
        syms = records.loc[need_sell, 'syms'].to_list()
        for sym in syms:
            order_target(sym, 0, real_time=real_time)  # 清仓
        records[need_sell, 'stage'] = 5
        logger.warning(f"{data.current_dt}, real_time: {real_time}, 卖出{syms}")

    # 第二次涨停的数据
    second_limit_up = records['stage'] == 3
    if second_limit_up.any():
        # 买入的条件：1.第一次涨停维持时间超过T秒，2.第一次涨停开始时间小于TM时刻，3.第二次涨停瞬间的tick的交易量位于当日集合竞价到第二次涨停瞬间所有tick量的前N位
        other_condition = (records['count'] >= context.t) & (records['first_time'].dt.time < context.tm) & \
            (records.apply(lambda x: x['volume_list'] and x['volume_list'][-1] <= x['volume'], axis=1))
        need_order = other_condition & second_limit_up
        if need_order.any():
            # 依次按照固定比例买入
            syms = records.loc[need_order, 'syms'].to_list()
            for sym in syms:
                order_target_percent(sym, context.w, real_time=real_time)
            logger.warning(f"[{context.i}]{data.current_dt}, real_time: {real_time}, 买入{syms}")
            records.loc[need_order, 'stage'] = 4
        # 不满足买入条件的结束生命周期
        records.loc[(~other_condition) & second_limit_up, 'stage'] = 5
        records.loc[second_limit_up, 'volume_list'] = records.loc[second_limit_up, 'volume_list'].apply(lambda x: [])  # 清空交易数据

    context.records = records.drop('price', axis=1).drop('volume', axis=1)
    logger.info(f"[{context.i}]{data.current_dt}, real_time: {real_time}, 耗时：{time.time() - start}s, 耗时1：{round(time_stampe1 - start, 2)}  耗时2：{round(time_stampe2 - time_stampe1, 2)}s ，剩余cash:{context.portfolio.cash}")


# def test():
#     dt = pd.Timestamp('2024-10-09 09:31:00', tz=get_localzone())
#     for sym in syms:
#         ret = data.get_spot_value(sym, 'price', dt, 'minute')

# def initialize(context):
#     context.i = 0
#     context.syms = syms
#     context.open_time = calendar.open_times[0][1]
#     context.close_time = calendar.close_times[0][1]
#     context.break_start_time = calendar.break_start_times[0][1]
#     context.break_end_time = calendar.break_end_times[0][1]
#     context.middle = asset_cnt // 2


# def handle_data(context, data):
#     if context.i == 0:
#         data.current(context.syms, 'volumn')

#     context.i += 1
#     cur_time = data.current_dt.time()
#     if cur_time < context.open_time or cur_time > context.close_time or context.break_start_time < cur_time <= context.break_end_time:
#         return

#     start = time.time()
#     prices = data.current(context.syms, 'price')
#     prices = prices[prices.notnull()].sort_values()
#     if prices.empty:
#         return
#     read_time = time.time()
#     real_time = data.current(prices.index[0], 'real_time')
#     middle = len(prices) // 2
#     sort_time = time.time()
#     sym = prices.index[middle]
#     median_price = prices[middle]
#     available_cash = context.portfolio.cash
#     shares_to_buy = int(available_cash // median_price)
#     if shares_to_buy > 0:
#         order_target(sym, shares_to_buy, real_time=real_time)
#     logger.info(f"{data.current_dt} 全仓买入 {shares_to_buy} 股 {sym}，总耗时：{time.time() - start}s，读取耗时：{read_time - start}，排序耗时：{sort_time - read_time}，available_cash：{available_cash}")
