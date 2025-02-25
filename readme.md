# zipline回测优化框架

本项目基于zipline框架，进行了一定程度的性能和功能优化

## 特性

- ingest性能优化：从串行处理改造为多进程并行的方式
- 回测性能优化：原框架耗时点在：1、ExchangeCalendar.minute_to_session；2、asset.start_date.tz_localize(dt.tzinfo)；3、bcolz的缓存大小默认为3000，如果股票数大于3000，会导致每次回测都要读盘，严重拉低性能。上述问题通过增加缓存，达到每次回测时间在0.05s左右
- 国内股票回测支持：原框架的时间都是当成UTC时间处理，不符合国内时间习惯，改成根据服务器时区处理时间。
- 新增环境变量配置项：
  1. *ZIPLINE_ROOT*：ingest后数据的存放路径（用于系统盘不够大时情况下，将数据存到其他盘）；
  2. *TEMPDIR*：ingest时临时文件存放路径（用于系统盘不够大时情况下，将数据存到其他盘）；
  3. *fields*：OHLCV可配置，支持ingest以及回测时可以新增字段，不配置的话默认为原来的'open,high,low,close,volume'，配置格式为英文逗号分隔字段;
  4. *lru_size*：bcolz的缓存大小，默认为3000，如果股票数大于3000，会导致每次回测都要读盘，严重拉低性能。

## 使用方法

1. 安装依赖包：pip install -r requirements.txt
2. - 您可以从trade.csv生成秒级数据（即second_bar数据），如下，从20240101到20240104（含）的数据进行统计：

     ``python3 main.py --type statistic --start_date 20240101 --end_date 20240104``
   - 您也可以生成伪造的数据，如下, 生成60天的测试数据：

     ``python3 main.py --type gen_test --days 60``

   生成的second_bar csv数据默认在minute目录下，**同时生成date_map.csv文件记录真实时间到映射时间的关系表，内容格式如下**
   ```
   真实时间戳,真实时间,映射时间
   1706751001,2024-02-01 09:30:01,1991-01-02 09:31:00
   1706751002,2024-02-01 09:30:02,1991-01-02 09:32:00
   1706751003,2024-02-01 09:30:03,1991-01-02 09:33:00
   1706751004,2024-02-01 09:30:04,1991-01-02 09:34:00
   ```
3. 执行ingest：
   ``python3 main.py --type ingest``
4. 执行回测：
   - 您可以不指定回测的起始日期和结束日期，默认为回测所有数据：
   ``python3 main.py --type algo``
   - 您也可以指定回测的起始日期和结束日期（日期为映射日期）：
  ``python3 main.py --type algo --start 20240101 --end 20240104``
   - 您也可以进行按天的并行回测：
  ``python3 main.py --type algo --parallel``


更多具体的用法查看help

```
python3 main.py --help
usage: main.py [-h] [--start START] [--end END] [--type {statistic,decompress,ingest,algo,gen_test}] [--days DAYS] [--rootdir ROOTDIR] [--csvdir CSVDIR] [--tempdir TEMPDIR] [--zipline_root ZIPLINE_ROOT] [--fields FIELDS] [--lru_size LRU_SIZE]

基于zipline的回测框架。支持从trade.csv生成分钟级数据，并进行ingest和回测

options:
  -h, --help            show this help message and exit
  --start START         从该起始日期统计trade.csv，仅在type为statistic有效，非必填
  --end END             统计trade.csv的结束日期，仅在type为statistic有效，非必填
  --type {statistic,decompress,ingest,algo,gen_test}
                        操作类型：statistic为从trade.csv统计分钟级数据；decompress为解压出trade.csv；ingest为执行ingest操作；algo为运行回测；gen_test为生成测试数据。默认algo
  --parallel            启用按天并行回测
  --days DAYS           type为gen_test有效，生成多少天的数据，非必填
  --rootdir ROOTDIR     数据根目录，默认/data/sse/
  --csvdir CSVDIR       生成的分钟级数据目录，默认./minute
  --tempdir TEMPDIR     zipline ingest过程中的临时目录，默认/data/zipline/tmp/
  --zipline_root ZIPLINE_ROOT
                        zipline数据的目录，默认/data/zipline
  --fields FIELDS       zipline ingest过程中使用的字段，默认open,high,low,close,volume,real_time
  --lru_size LRU_SIZE   zipline回测过程中使用的lru_size，默认6000
```
