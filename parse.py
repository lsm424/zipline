# encoding=utf-8

# grep -nR 二次不满足买入的数据 zipline.log -A 1|grep -v 二次不满足买入的数据 > no.txt
import re
import ast
import os
from datetime import datetime
pattern = r'^\S+\s+(\d+)\s+(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s+(\d+)\s+(\[.*?\])\s+(\d+\.\d+)'

os.system('grep -nR 二次不满足买入的数据 zipline.log -A 2|grep -v 二次不满足买入的数据 > no.txt')
data = open('no.txt', 'r').read().split('\n')
all_cnt = 0
first_time_cnt = 0
count_cnt = 0
volume_cnt = 0
tm = datetime(year=1970, month=1, day=1, hour=11, minute=0, second=0).time()
for line in data:
    match = re.search(pattern, line)
    if not match:
        continue
    stock, first_time, count, volume_list, volume = match.groups()
    first_time = datetime.strptime(first_time, '%Y-%m-%d %H:%M:%S')
    count = int(count)
    volume_list = ast.literal_eval(volume_list)
    volume = float(volume)
    all_cnt += 1
    if first_time.time() < tm:
        first_time_cnt += 1
    if count < 1800:
        count_cnt += 1
    if volume_list and volume_list[0] > volume:
        volume_cnt += 1
print(all_cnt, first_time_cnt / all_cnt, count_cnt / all_cnt, volume_cnt / all_cnt)
