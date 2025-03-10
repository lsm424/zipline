from datetime import datetime
import pandas as pd
import time


def gen_fake_data(times: int, limit_up_price: float, stock: str):
    data = []

    # 定义股市的开市时间段
    morning_start = "09:30:00"
    morning_end = "11:30:00"
    afternoon_start = "13:00:00"
    afternoon_end = "15:00:00"

    # 获取当前日期的午夜时间（00:00:00）
    today = datetime.today().date()
    midnight = datetime.combine(today, datetime.min.time())

    # 将股市开盘时间转换为当天的时间
    morning_start_time = datetime.strptime(morning_start, "%H:%M:%S")
    morning_end_time = datetime.strptime(morning_end, "%H:%M:%S")
    afternoon_start_time = datetime.strptime(afternoon_start, "%H:%M:%S")
    afternoon_end_time = datetime.strptime(afternoon_end, "%H:%M:%S")

    # 计算这些时间相对于午夜的秒数
    morning_start_seconds = int((morning_start_time - midnight).total_seconds())
    morning_end_seconds = int((morning_end_time - midnight).total_seconds())
    afternoon_start_seconds = int((afternoon_start_time - midnight).total_seconds())
    afternoon_end_seconds = int((afternoon_end_time - midnight).total_seconds())
    # 初始时间从 09:30:00 开始
    current_time_seconds = morning_start_seconds

    for _ in range(times):
        # 固定列
        year = 2011
        col_1 = 1
        col_2 = stock
        col_5 = limit_up_price
        col_6 = 10000

        # 计算当前时间
        time_str = time.strftime("%H%M%S", time.gmtime(current_time_seconds))
        final_time = time_str

        # 添加这一行数据
        data.append([year, col_1, col_2, final_time, col_5, col_6])

        # 增加1秒
        current_time_seconds += 1

        # 检查是否跨越时间段
        if current_time_seconds > morning_end_seconds and current_time_seconds < afternoon_start_seconds:
            # 上午到下午
            current_time_seconds = afternoon_start_seconds
        elif current_time_seconds > afternoon_end_seconds:
            # 超过下午时间段，重置为上午9:30
            current_time_seconds = morning_start_seconds

    # 转换为 DataFrame
    df = pd.DataFrame(data, columns=["Year", "Column1", "Stock", "Time", "LimitUpPrice", "Column6"])

    last_row = df.iloc[-1].copy()  # 获取最后一行数据
    new_rows = []

    # 第一行: LimitUpPrice 小1，时间递增
    last_time = last_row["Time"]
    last_limit_up_price = last_row["LimitUpPrice"]
    new_time = (datetime.strptime(last_time, "%H%M%S") + pd.Timedelta(seconds=1)).strftime("%H%M%S")

    new_rows.append([2011, 1, stock, new_time, last_limit_up_price - 1000, 10000])

    # 第二行: LimitUpPrice 等于原值，时间递增
    new_time = (datetime.strptime(new_time, "%H%M%S") + pd.Timedelta(seconds=1)).strftime("%H%M%S")
    new_rows.append([2011, 1, stock, new_time, limit_up_price, 10000])

    # 将新增的两行数据添加到 DataFrame
    df = pd.concat([df, pd.DataFrame(new_rows, columns=["Year", "Column1", "Stock", "Time", "LimitUpPrice", "Column6"])])
    df['Time'] = df['Time'] + '12'
    return df


if __name__ == '__main__':
    # 7840,2024-04-03,10.53,10.55,10.42,10.46,98184585.0,1028648883.0,19405546950.0,0.0050596144109197605,000001

    # 测试函数
    times = 1801
    limit_up_price = 11.88 * 10000
    stock = "000001"
    df = gen_fake_data(times, limit_up_price, stock)
    print(df)
    df.to_csv('trade.csv', header=False, index=False)
