import vaex
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
import pandas as pd

# 数据库连接URL
DATABASE_URL = "sqlite:///second3.db"  # SQLite 数据库文件

# 定义基础类
Base = declarative_base()


def to_dict(self):
    if not hasattr(self, 'serialize_only'):
        self.serialize_only = list(map(lambda x: x.name, self.__table__.columns))
    return {c: getattr(self, c, None) for c in self.serialize_only}


Base.to_dict = to_dict

# 定义模型类

'''CREATE TABLE second3 (
    id INTEGER PRIMARY KEY AUTOINCREMENT, 
    time TIMESTAMP NOT NULL,
    stock varchar(100) NOT NULL,
    price float NOT NULL, 
    volumn int not null,
    UNIQUE (time, stock)  -- `name` 和 `age` 作为唯一约束
);'''


class Second3(Base):
    __tablename__ = 'second3'  # 表名

    id = Column(Integer, primary_key=True, autoincrement=True)  # 自增主键
    time = Column(TIMESTAMP, nullable=False)  # 时间戳，不能为空
    stock = Column(String(100), nullable=False)  # 股票名称，不能为空
    price = Column(Float, nullable=False)  # 价格，不能为空
    volumn = Column(Integer, nullable=False)  # 成交量，不能为空

    # 定义唯一约束
    __table_args__ = (
        UniqueConstraint('time', name='uq_time'),  # time 和 stock 作为唯一约束
    )


# engine = create_engine(DATABASE_URL, echo=False)
# # 创建表（如果不存在）
# Base.metadata.create_all(engine)
# # 创建会话
# SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
# session = SessionLocal()

db_dir = 'db'
os.makedirs(db_dir, exist_ok=True)
sessions = {}


def get_session(stock):
    if stock in sessions:
        return sessions[stock]

    DATABASE_URL = f"sqlite:///{db_dir}/{stock}.db"  # SQLite 数据库文件

    engine = create_engine(DATABASE_URL, echo=False)
    # 创建表（如果不存在）
    Base.metadata.create_all(engine)
    # 创建会话
    session = sessionmaker(autocommit=False, autoflush=False, bind=engine)()
    sessions[stock] = session
    return session


def insert(batch):
    batch = sorted(batch, key=lambda x: x['stock'])
    batch = groupby(batch, key=lambda x: x['stock'])

    def _insert(stock, batches):
        session = get_session(stock)
        # batches = list({item['time']: item for item in batches}.values())
        insert(session, batches)
        # print(f'insert {stock} {len(batches)}')
    executor = ThreadPoolExecutor(max_workers=100)
    # r = list(map(lambda x: executor.submit(_insert, x[0], x[1]), batch))
    r = []
    for stock, batches in batch:
        batches = list({item['time']: item for item in batches}.values())
        r.append(executor.submit(_insert, stock, batches))
    wait(r, return_when=ALL_COMPLETED)

    try:
        stmt = sqlite_insert(Second3).values(batch)
        stmt = stmt.on_conflict_do_nothing(index_elements=['time'])  # 指定冲突处理
        session.execute(stmt)
        session.commit()
        # session.bulk_insert_mappings(Second3, batch)
        # session.commit()
    except Exception as e:
        print(e)
        session.rollback()


def decompress_bz2(source_path):
    destination_path = os.path.splitext(source_path)[0] + '.csv'
    destination_path = os.path.dirname(source_path)
    shutil.unpack_archive(source_path, destination_path, format='bztar')
    # with bz2.open(source_path, 'rb') as source, open(destination_path, 'wb') as destination:
    #     for data in source:
    #         destination.write(data)
    print(f'finish {destination_path}')
    return destination_path


def find_files(rootdir, name='snap.csv.tar.bz2', start=0, count=60):
    all_files = []
    for root, dirs, files in os.walk(rootdir):
        if name not in files:
            continue
        all_files.append(os.path.join(root, name))
    all_files = sorted(all_files, reverse=True)[start:count + start]
    print(f'start {all_files}')
    return all_files


def decompress_dir(rootdir):
    all_files = find_files(rootdir, start=60, count=10)
    with multiprocessing.Pool(processes=10) as pool:  # 创建含有4个进程的进程池
        all_files = pool.map(decompress_bz2, all_files)
    return all_files


def insert_data(csv_filename):
    date = os.path.basename(os.path.dirname(csv_filename))
    with open(csv_filename, newline='') as csvfile:
        csv_reader = csv.reader(csvfile)
        batch = []
        for idx, data in enumerate(csv_reader):
            if len(data[0]) > 6:
                continue
            data[0] = data[0].zfill(6)
            if not ((data[0] >= '093000' and data[0] <= '113000') or (data[0] >= '130000' and data[0] <= '150000')):
                continue
            time = datetime.strptime(f'{date}{data[0]}', '%Y%m%d%H%M%S')
            batch.append({
                'time': time,
                'stock': int(data[1]),
                'price': float(data[2]),
                'volumn': int(data[8]),
            })
            if len(batch) > 100000:
                batch = sorted(batch, key=lambda x: x['stock'])
                batch = groupby(batch, key=lambda x: x['stock'])

                def _insert(stock, batches):
                    session = get_session(stock)
                    # batches = list({item['time']: item for item in batches}.values())
                    insert(session, batches)
                    # print(f'insert {stock} {len(batches)}')
                executor = ThreadPoolExecutor(max_workers=100)
                # r = list(map(lambda x: executor.submit(_insert, x[0], x[1]), batch))
                r = []
                for stock, batches in batch:
                    batches = list({item['time']: item for item in batches}.values())
                    r.append(executor.submit(_insert, stock, batches))
                wait(r, return_when=ALL_COMPLETED)
                print(idx)
                batch = []

        insert(batch)


def analys(file, date=None):
    date = os.path.basename(os.path.dirname(file)) if not date else date
    df = vaex.from_csv('your_file.csv', names=['time', 'stock', 'm_pre_close_price', 'm_open_px', 'm_day_high', 'm_day_low', 'm_last_px', 'm_total_trade_number',
                                               'm_total_qty', 'm_total_value', 'm_total_bid_qty', 'm_total_bid_weighted_avg_px', 'm_total_ask_qty', 'm_total_ask_weighted_avg_Px',
                       'm_bid_depth'], convert=True)
    df = df[df['time'].str.len() <= 6]
    df['time'] = df['time'].str.zfill(6)
    # 确保 time 列是 datetime 类型
    df['time'] = date[:2] + '-' + date[2:4] + '-' + date[4:6] + ' ' + df['time'].str[:2] + ':' + df['time'].str[2:4] + ':' + df['time'].str[4:6]
    df['time'] = pd.to_datetime(df['time'])
    # 将 time 设置为索引
    df.set_index('time', inplace=True)

    # 提取分钟，创建一个新的列用于分组（保留整分钟）
    df['minute'] = df.index.floor('T')

    # 按 stock 和 minute 分组
    grouped = df.groupby(['stock', 'minute'])

    # 计算所需的统计信息
    result = grouped.agg(
        max_price=('price', 'max'),
        min_price=('price', 'min'),
        min_time_price=('price', 'idxmin'),  # 最小时间对应的 price
        max_time_price=('price', 'idxmax')   # 最大时间对应的 price
    ).reset_index()

    # 输出结果
    print(result)


if __name__ == '__main__':
    rootdir = '/media/USBDISK/sse/'
    # print(decompress_dir(rootdir))
    analys('snap.csv', '20240301')
    # all_files = find_files(rootdir, 'snap.csv')
    # insert_data(f'/media/USBDISK/sse/20240430/snap.csv')
    # insert_data('20240630', 'snap.csv')
