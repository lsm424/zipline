import os
import vaex
import time
import numpy as np
import h5py
from datetime import datetime, timedelta


class ReadHdf5Bar:
    def __init__(self, file_dir) -> None:
        self.file_dir = file_dir
        dirs = list(filter(lambda x: x.endswith('.csv.hdf5'), os.listdir(file_dir)))
        self.min_date = datetime.strptime(min(dirs).split('.')[0], "%Y%m%d")
        self.max_date = datetime.strptime(max(dirs).split('.')[0], "%Y%m%d")

    def query(self, start_date=None, end_date=None, stock=None, ret_cols=['stock', 'time', 'high', 'low', 'open', 'stock', 'volume']):
        start_date = datetime.strptime(start_date, "%Y%m%d") if start_date else self.min_date
        end_date = (datetime.strptime(end_date, "%Y%m%d") if end_date else self.max_date) + timedelta(days=1)
        for i in range((end_date - start_date).days):
            date = (start_date + timedelta(days=i)).strftime("%Y%m%d")
            data = self._query_date(date, ret_cols, stock)
            if data is not None:
                yield data

    def _query_date(self, date,  ret_cols, stock=None):
        file = os.path.join(self.file_dir, f'{date}.csv.hdf5')
        if not os.path.exists(file):
            return None

        f = h5py.File(file, 'r')
        if stock:
            filtered_indices = []
            stock_dataset = f['table']['columns']['stock']['data']
            indices_in_chunk = np.where(stock_dataset[:] == stock)[0]
            filtered_indices.extend(indices_in_chunk)

        filtered_data = {}
        for key in ret_cols:
            dataset = f['table']['columns'][key]['data']
            filtered_data[key] = dataset[filtered_indices] if stock else dataset[:]

        return filtered_data

    def print_structure(self, group, indent=0):
        for key in group:
            item = group[key]
            if isinstance(item, h5py.Group):
                print("  " * indent + f"Group: {key}")
                self.print_structure(item, indent + 1)  # 递归进入子组
            elif isinstance(item, h5py.Dataset):
                print("  " * indent + f"Dataset: {key}, Shape: {item.shape}, Dtype: {item.dtype}")
            else:
                print("  " * indent + f"Unknown item: {key}")


bar = ReadHdf5Bar('./result/1s_bar')
# file = './result/1s_bar/20231226.csv.hdf5'
# f = h5py.File(file, 'r')
# bar.print_structure(f)
columns = ['stock', 'time', 'high', 'low', 'open', 'stock', 'volume']


start_time = time.time()
for one_day_data in bar.query(stock=688778):
    print(one_day_data, one_day_data['stock'].shape)
    hdf5_filter_time = time.time() - start_time
    print(f"HDF5 过滤stock=688778时间: {hdf5_filter_time} 秒")
hdf5_filter_time = time.time() - start_time
print(f"HDF5 过滤stock=688778时间: {hdf5_filter_time} 秒")

start_time = time.time()
for one_day_data in bar.query():
    print(one_day_data, one_day_data['stock'].shape)
hdf5_filter_time = time.time() - start_time
print(f"HDF5查询全量数据的时间: {hdf5_filter_time} 秒 ")


# 1917万行数据，20231226这一天的秒级数据
hdf5_df = vaex.open(file)
start_time = time.time()
# 筛选股票688778的数据，耗时0.026秒
filtered_hdf5 = hdf5_df[hdf5_df['stock'] == 688778]
hdf5_filter_time = time.time() - start_time
print(f"HDF5过滤时间: {hdf5_filter_time} 秒 {hdf5_df.shape} {filtered_hdf5.shape}")

# 测试从HDF5读取数据并进行遍历操作的性能
start_time = time.time()
for row in filtered_hdf5.iter():
    pass
hdf5_iter_time = time.time() - start_time
print(f"HDF5遍历时间: {hdf5_iter_time} 秒 ")

# # 测试从CSV读取数据并进行过滤操作的性能
# start_time = time.time()
# csv_df = vaex.open('data.csv')
# filtered_csv = csv_df[csv_df.x > 0.5]
# csv_filter_time = time.time() - start_time
# print(f"CSV过滤时间: {csv_filter_time} 秒")

# # 测试从CSV读取数据并进行遍历操作的性能
# start_time = time.time()
# for row in csv_df.iterrows():
#     pass
# csv_iter_time = time.time() - start_time
# print(f"CSV遍历时间: {csv_iter_time} 秒")
