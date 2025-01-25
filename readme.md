# zipline回测优化框架
本项目基于zipline框架，进行了一定程度的性能和功能优化
## 特性
- ingest性能优化：从串行处理改造为多进程并行的方式
- 回测性能优化：原框架耗时点在：1、ExchangeCalendar.minute_to_session；2、asset.start_date.tz_localize(dt.tzinfo)；3、bcolz的缓存大小默认为3000，如果股票数大于3000，会导致每次回测都要读盘，严重拉低性能。上述问题通过增加缓存，达到每次回测时间在0.05s左右
- 数据目录修改支持：原框架默认在~/.zipline中存放数据，如果系统盘不够大，则无法回测。为了增加灵活性，本项目新增环境变量：ZIPLINE_ROOT：ingest后数据的存放路径；TEMPDIR：ingest时临时文件存放路径。
- 国内股票回测支持：原框架的时间都是当成UTC时间处理，不符合国内时间习惯，改成根据服务器时区处理时间。


#【todo】
- OHLCV可配置，ingest可以新增字段