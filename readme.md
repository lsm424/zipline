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
  
  