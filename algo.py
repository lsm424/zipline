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
from zipline.api import order_target, order_target_percent, order_target_value, record, symbol, set_symbol_lookup_date
from zipline.data.data_portal import DataPortal
from zipline.utils.calendar_utils import get_calendar
from zipline.api import order, record, symbol
from time import time
from numpy import searchsorted
from pandas import notna
import os
from datetime import datetime, timedelta
from numba import jit

if os.environ.get('ZIPLINE_ROOT') is None:
    os.environ['ZIPLINE_ROOT'] = '/data/zipline'


#######################################################################
# 回测算法，根据自己的需求修改下面的函数
#######################################################################
def initialize(context):
    context.data_portal
    # 初始化
    context.i = 0  # 帧数统计
    # 筛选60或00开头代码
    context.records_pd = context.records_pd[context.records_pd['stock'].str.startswith(('60', '00'))]
    # 记录当前日期，用于判是否跨天
    context.cur_date = None

    context.t = 1800  # 条件1：超过涨停价的秒数配置
    context.tm = datetime(year=1970, month=1, day=1, hour=11, minute=0, second=0).time()  # 条件2：第一次超过涨停价的时间
    context.pre_n = 5  # 条件3：第二次涨停瞬间的tick的交易量位于当日集合竞价到第二次涨停瞬间所有tick量的前N位的配置
    context.w = 1/3  # 配置买入的权重百分比
    context.default_date = datetime(year=1970, month=1, day=1, hour=0, minute=0, second=0)
    context.records_pd['count'] = 0  # 统计第一次涨停的时长
    context.records_pd['first_time'] = [context.default_date] * len(context.records_pd)   # 统计第一次涨停时间
    context.records_pd['volume_list'] = [[]] * len(context.records_pd)  # 标记生命周期是否结果
    context.records_pd['stage'] = 0  # 标记阶段：0 等待涨停；1 第一次涨停中； 2 第一次涨停结束；3 第二次涨停；4 已买入；5 已卖出/已结束
    context.records_pd['value'] = 0  # 记录昨天收盘价下的总价值
    logger.info(f'开始回测{len(context.records_pd)}只股票，初始金额{context.portfolio.cash}，日期：{context.sim_params.start_session} - {context.sim_params.end_session}')


def handle_data(context, data):
    context.i += 1
    start = time()
    # 当前帧股价
    prices = data.current(context.syms, 'price')
    prices = prices[prices.notnull()]
    if prices.empty:  # 当前帧所有股票数据为空
        logger.warning(f"[{context.i}]{data.current_dt}，所有股票价格为空，跳过，剩余cash：{context.portfolio.cash}")
        return

    # 获得真实时间
    real_time = datetime.fromtimestamp(data.current(prices.index[0], 'real_time'))

    # 如果是新的一天，则更新records中的涨停价、跌停价，以及其他字段初始化
    cur_date = real_time.date()
    if context.cur_date != cur_date:
        context.cur_date = cur_date
        date = pd.to_datetime(cur_date.strftime("%Y-%m-%d"))

        # 筛选每个股票的上一个交易日的数据
        last_day_trades = context.get_latest_trade_data(date)
        # 得到今天的涨停价、跌停价阈值
        records_pd = context.records_pd.merge(last_day_trades[['stock', 'close']], how='left', on='stock')
        records_pd['limit_up_price'] = records_pd['close'] * 1.1  # 涨停价
        records_pd['limit_down_price'] = records_pd['close'] * 0.9  # 跌停价
        records_pd.loc[records_pd['stage'] != 4, 'count'] = 0
        records_pd.loc[records_pd['stage'] != 4, 'first_time'] = context.default_date
        records_pd.loc[records_pd['stage'] != 4, 'stage'] = 0
        records_pd.loc[records_pd['stage'] != 4, 'volume_list'] = records_pd.loc[records_pd['stage'] != 4, 'volume_list'].apply(lambda x: [])
        context.records_pd = records_pd.drop('close', axis=1)
        # 当天每一只股票的购买金额上线
        context.max_cash_per_order = context.portfolio.cash * context.w

    # 按stock名称，统计交易量，价格合并到records
    prices = prices.reset_index()
    prices['stock'] = prices['index'].map(lambda x: x.symbol)
    smys = context.records_pd.loc[(context.records_pd['stage'] <= 3) & context.records_pd['stock'].isin(prices['stock'])]['syms'].to_list()
    volume = data.current(smys, 'volume').reset_index()
    volume['stock'] = volume['index'].map(lambda x: x.symbol)
    records_pd = context.records_pd.merge(prices[['stock', 'price']], on='stock', how='left').merge(volume[['stock', 'volume']], on='stock', how='left')
    time_stampe1 = time()

    def keep_n_volume(row, pre_n):  # 将volume插入到有序的volume_list中，并保持前pre_n个值
        if notna(row['volume']):
            insert_pos = searchsorted(row['volume_list'], row['volume'])
            if insert_pos > 0 or len(row['volume_list']) < pre_n:
                start_pos = 0 if len(row['volume_list']) < pre_n else 1
                row['volume_list'] = row['volume_list'][start_pos:insert_pos] + [row['volume']] + row['volume_list'][insert_pos:]
        return row['volume_list']

    # 转换volume_list为numpy数组，并使用apply的方式
    pre_n = context.pre_n
    records_pd['volume_list'] = records_pd.apply(lambda x: keep_n_volume(x, pre_n), axis=1)
    time_stampe2 = time()

    # 超过涨停价的数据索引
    mask_ge = records_pd['price'] >= records_pd['limit_up_price']
    # 之前是未涨停的阶段，则 stage 加1
    records_pd.loc[mask_ge & (records_pd['stage'].isin([0, 2])), 'stage'] += 1
    # 第一次涨停时间为空则设置为当前时间
    records_pd.loc[mask_ge & (records_pd['first_time'] == context.default_date), 'first_time'] = real_time
    # 第一次的连续涨停次数加1
    records_pd.loc[mask_ge & (records_pd['stage'] == 1), 'count'] += 1

    # 未超过涨停价的数据索引
    mask_lt = records_pd['price'] < records_pd['limit_up_price']
    # 之前是涨停的阶段，则 stage 加1
    records_pd.loc[mask_lt & (records_pd['stage'] == 1), 'stage'] += 1

    # 若为第二天，若盘中任意时间跌停，则按跌停价卖出，或10点整或14:57分不涨停，则按最新价卖出
    need_sell1 = (records_pd['stage'] == 4) & (records_pd['first_time'].dt.date == (real_time - timedelta(days=1)).date()) & ((records_pd['price'] <= records_pd['limit_down_price']) |
                                                                                                                              ((real_time.hour == 10) & (real_time.minute == 0) & (real_time.second == 0) & (records_pd['price'] < records_pd['limit_up_price'])) |
                                                                                                                              ((real_time.hour == 14) & (real_time.minute == 57) & (real_time.second == 0) & (records_pd['price'] < records_pd['limit_up_price'])))
    # 若为第三天及以后，盘中任意时间跌停，则按跌停价卖出，或14:57分不涨停，则按最新价卖出
    need_sell2 = (records_pd['stage'] == 4) & (records_pd['first_time'].dt.date < (real_time - timedelta(days=1)).date()) & ((records_pd['price'] <= records_pd['limit_down_price']) |
                                                                                                                             ((real_time.hour == 14) & (real_time.minute == 57) & (real_time.second == 0) & (records_pd['price'] < records_pd['limit_up_price'])))
    need_sell = need_sell1 | need_sell2
    if need_sell.any():  # 售出操作
        logger.warning(f"[{context.i}]{data.current_dt}, real_time: {real_time}, 待卖出：{records_pd.loc[need_sell][['stock', 'first_time', 'count', 'volume_list', 'volume', 'price']]}")
        syms = records_pd.loc[need_sell, 'syms'].to_list()
        for sym in syms:
            order_target(sym, 0, real_time=real_time)  # 清仓
        records_pd.loc[need_sell, 'stage'] = 5
        logger.warning(f"[{context.i}]{data.current_dt}, real_time: {real_time}, 卖出{syms}")

    # 第二次涨停的数据
    second_limit_up = records_pd['stage'] == 3
    if second_limit_up.any():
        # 买入的条件：1.第一次涨停维持时间超过T秒，2.第一次涨停开始时间小于TM时刻，3.第二次涨停瞬间的tick的交易量位于当日集合竞价到第二次涨停瞬间所有tick量的前N位
        other_condition = (records_pd['count'] >= context.t) & (records_pd['first_time'].dt.time < context.tm) & \
            (records_pd.apply(lambda x: x['volume_list'] and x['volume_list'][0] <= x['volume'], axis=1))
        need_order = other_condition & second_limit_up
        if need_order.any():
            # 依次按照固定金额买入
            cash = context.portfolio.cash
            syms = records_pd.loc[need_order, 'syms'].to_list()
            logger.warning(f"待买入：{records_pd.loc[need_order][['stock', 'first_time', 'count', 'volume_list', 'volume', 'price']]}")
            for sym in syms:
                if cash < context.max_cash_per_order:
                    logger.warning(f"[{context.i}]{data.current_dt}, real_time: {real_time}, 剩余cash不足 {cash}")
                    records_pd.loc[records_pd['stock'] == sym.symbol, 'stage'] = 5
                    continue
                order_target_value(sym, context.max_cash_per_order, real_time=real_time)
                # logger.warning(f"[{context.i}]{data.current_dt}, real_time: {real_time}, 成功买入{syms}")
                cash -= context.max_cash_per_order
                records_pd.loc[records_pd['stock'] == sym.symbol, 'stage'] = 4
        # 不满足买入条件的结束生命周期
        records_pd.loc[(~other_condition) & second_limit_up, 'stage'] = 5
        # records_pd.loc[second_limit_up, 'volume_list'] = records_pd.loc[second_limit_up, 'volume_list'].apply(lambda x: [])  # 清空交易数据

    context.records_pd = records_pd.drop('price', axis=1).drop('volume', axis=1)
    logger.info(f"[{context.i}]{data.current_dt}, real_time: {real_time}, 耗时：{round(time() - start, 3)}s, 耗时1：{round(time_stampe1 - start, 3)}  耗时2：{round(time_stampe2 - time_stampe1, 3)}s ，剩余cash:{context.portfolio.cash}")

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

#     start = time()
#     prices = data.current(context.syms, 'price')
#     prices = prices[prices.notnull()].sort_values()
#     if prices.empty:
#         return
#     read_time = time()
#     real_time = data.current(prices.index[0], 'real_time')
#     middle = len(prices) // 2
#     sort_time = time()
#     sym = prices.index[middle]
#     median_price = prices[middle]
#     available_cash = context.portfolio.cash
#     shares_to_buy = int(available_cash // median_price)
#     if shares_to_buy > 0:
#         order_target(sym, shares_to_buy, real_time=real_time)
#     logger.info(f"{data.current_dt} 全仓买入 {shares_to_buy} 股 {sym}，总耗时：{time() - start}s，读取耗时：{read_time - start}，排序耗时：{sort_time - read_time}，available_cash：{available_cash}")
