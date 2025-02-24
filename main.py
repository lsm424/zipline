import argparse
import os
from exchange_calendars import get_calendar
import pandas as pd
from gen_test_data import gen_stock
import time
import loguru
from exchange_calendars import get_calendar
from statistic_trade_csv import decompress_dir, statistic_trade, calendar  # NOQA: E402

from model import get_data_by_strategy

loguru.logger.add("./zipline.log", colorize=False, level="INFO", encoding="utf-8", retention="5 days", rotation="1 day", enqueue=True)
logger = loguru.logger

parser = argparse.ArgumentParser(description='基于zipline的回测框架。支持从trade.csv生成分钟级数据，并进行ingest和回测')
parser.add_argument('--start', type=str, default=None, help='从该起始日期统计trade.csv，仅在type为statistic有效，非必填', required=False)
parser.add_argument('--end', type=str, default=None, help='统计trade.csv的结束日期，仅在type为statistic有效，非必填', required=False)
parser.add_argument('--type', type=str, default='algo', help='操作类型：statistic为从trade.csv统计分钟级数据；decompress为解压出trade.csv；ingest为执行ingest操作；algo为运行回测；gen_test为生成测试数据。默认algo',
                    choices=['statistic', 'decompress', 'ingest', 'algo', 'gen_test'])
parser.add_argument('--days', type=int, default=60, help='type为gen_test有效，生成多少天的数据，非必填', required=False)
parser.add_argument('--decompress_file', type=str, default='trade_data.csv.tar.bz2', help='解压的文件名，在type为decompress时生效', required=False)
parser.add_argument('--rootdir', type=str, default='/data/sse/', help='数据根目录，默认/data/sse/', required=False)
parser.add_argument('--csvdir', type=str, default='./minute', help='生成的秒级数据目录，默认./minute', required=False)
parser.add_argument('--tempdir', type=str, default='/data/zipline/tmp/', help='zipline ingest过程中的临时目录，默认/data/zipline/tmp/', required=False)
parser.add_argument('--zipline_root', type=str, default='/data/zipline', help='zipline数据的目录，默认/data/zipline', required=False)
parser.add_argument('--fields', type=str, default='open,high,low,close,volume,real_time', help='zipline ingest过程中使用的字段，默认open,high,low,close,volume,real_time', required=False)
parser.add_argument('--lru_size', type=str, default='6000', help='zipline回测过程中使用的lru_size，默认6000', required=False)
args = parser.parse_args()

if __name__ == '__main__':
    start_time = time.time()
    run_type = args.type
    if run_type == 'statistic':
        logger.info(f'开始统计trade.csv，起始日期：{args.start}, 结束日期：{args.end}，保存到{args.csvdir}')
        start_date, end_date = args.start, args.end
        statistic_trade(start_date, end_date, args.csvdir, args.rootdir)
    elif run_type == 'decompress':
        logger.info(f'开始解压trade.csv，保存到{args.rootdir}')
        logger.info(decompress_dir(args.rootdir, 0, 0, args.decompress_file, 'trade.csv'))
    elif run_type == 'gen_test':
        logger.info(f'准备生成测试数据，共{args.days}天')
        gen_stock(args.days)
    elif run_type == 'ingest':
        logger.info(f'开始执行ingest操作，原始csv数据在{args.csvdir}, 保存到{args.zipline_root}，字段为{args.fields}')
        # 环境变量的设置要在zipline库引入之前
        os.environ["CSVDIR"] = os.path.dirname(args.csvdir)  # NOQA: E402
        os.environ["TEMPDIR"] = args.tempdir  # NOQA: E402
        os.environ['ZIPLINE_ROOT'] = args.zipline_root  # NOQA: E402
        os.environ['fields'] = args.fields    # NOQA: E402
        from zipline.data.bundles.csvdir import csvdir_bundle
        from zipline.data.bundles import register, ingest
        register(
            'csvdir',
            csvdir_bundle,
            calendar_name='XSHG',
            minutes_per_day=330,
        )
        ingest('csvdir', assets_versions=[1])
    elif run_type == 'algo':
        # result = CliRunner().invoke(run, ['-f', 'zipline_pro.py', '--trading-calendar', 'XSHG', '--start', '2025-10-09', '--end',
        #                                   '2025-12-31', '--data-frequency', 'minute', '--bundle', 'csvdir', '--benchmark-sid', '0', '-o', 'dma.pickle'])
        # 环境变量的设置要在zipline库引入之前
        os.environ['fields'] = args.fields    # NOQA: E402
        os.environ['lru_size'] = args.lru_size  # NOQA: E402
        os.environ['ZIPLINE_ROOT'] = args.zipline_root  # NOQA: E402
        from zipline import run_algorithm
        from algo import initialize, handle_data
        # args.start = '1997-06-12'
        logger.info(f'执行回测，fields: {args.fields}, lru size: {args.lru_size}')
        perf = run_algorithm(
            start=pd.Timestamp(args.start) if args.start else None,
            end=pd.Timestamp(args.end) if args.end else None,
            trading_calendar=calendar,
            initialize=initialize,
            handle_data=handle_data,
            data_frequency='minute',
            bundle='csvdir',
            output='dma.pickle',
            capital_base=10000000,
        )
        models = get_data_by_strategy()
        logger.info(f'回测结束，perf: {perf}, models:\n{models}')
    logger.info(f"type: {run_type}，耗时：{time.time() - start_time}s")


# mprof run main.py 启动内存实时采集
# mprof plot mprofile_20250107193820.dat  画内存曲线图
