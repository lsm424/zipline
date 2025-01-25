# zipline回测优化框架
- 本项目基于zipline框架，优化ingest和回测性能
- ingest：从串行处理改造为多进程并行的方式
- 回测：原框架耗时点在：1、ExchangeCalendar.minute_to_session；2、asset.start_date.tz_localize(dt.tzinfo)；3、bcolz的缓存大小默认为3000，如果股票数大于3000，会导致每次回测都要读盘，严重拉低性能。上述问题通过增加缓存，达到每次回测时间在0.05s左右

#【todo】
- OHLCV可配置，ingest可以新增字段