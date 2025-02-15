# encoding=utf-8
from contextlib import contextmanager
from loguru import logger
import pandas as pd
from sqlalchemy import MetaData, PrimaryKeyConstraint, Table, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Float, String, DateTime, Float, BigInteger, Text, Integer  # 区分大小写
from sqlalchemy.orm import sessionmaker
from datetime import datetime

base = declarative_base()


def to_dict(self):
    if not hasattr(self, 'serialize_only'):
        self.serialize_only = list(map(lambda x: x.name, self.__table__.columns))
    return {c: getattr(self, c, None) for c in self.serialize_only}


base.to_dict = to_dict


class StrategyDeal(base):
    __tablename__ = 't_strategy_deal'

    id = Column(BigInteger, primary_key=True, autoincrement=True, nullable=True, unique=True)
    trade_date = Column(String(10))
    trade_time = Column(String(10))
    strategy_code = Column(String(20))
    security_code = Column(String(20))
    direction = Column(Integer)
    deal_weight = Column(Float, nullable=False, default=0)
    deal_price = Column(Float, nullable=False, default=0)


engine = create_engine(f"mssql+pymssql://straWriter:straWriter@KAIKAIWIN/StrategyDB?charset=utf8",
                       connect_args={'tds_version': '7.0'}, echo=False)


def get_db_session(engine):
    DbSession = sessionmaker()
    DbSession.configure(bind=engine)
    return DbSession()


@contextmanager
def get_db_context_session(transaction=False, engine=engine):
    session = get_db_session(engine)

    if transaction:
        try:
            session.begin()
            yield session
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()
    else:
        try:
            yield session
        except:
            raise
        finally:
            session.close()


def __get_stock_type(stock_code: str):
    """判断股票ID对应的证券市场
    匹配规则
    ['50', '51', '60', '90', '110'] 为 sh
    ['00', '13', '18', '15', '16', '18', '20', '30', '39', '115'] 为 sz
    ['5', '6', '9'] 开头的为 sh，
    ['8']开头为bj，其余为 sz
    :param stock_code:股票ID, 若以 'sz', 'sh' 开头直接返回对应类型，否则使用内置规则判断
    :return 'sh' or 'sz'"""
    sh_head = ("50", "51", "60", "90", "110", "113", "118",
               "132", "204", "5", "6", "9", "7")
    bj_head = ('8', )
    if stock_code.isdigit():
        stock_code += ".SH" if stock_code.startswith(sh_head) else ".BJ" if stock_code.startswith(bj_head) else ".SZ"
    return stock_code


def insert_db(dt: pd.Timestamp, security_code: str, direction: int, deal_weight: float, deal_price: float):
    security_code = __get_stock_type(security_code)
    with get_db_context_session() as session:
        session.add(StrategyDeal(trade_date=dt.strftime('%Y%m%d'),
                                 trade_time=dt.strftime('%H%M%S'),
                                 strategy_code='db_bt_hf1',
                                 security_code=security_code,
                                 direction=direction,
                                 deal_weight=deal_weight,
                                 deal_price=deal_price))
        session.commit()
        logger.warning(f"trade_time: {dt}, code: {security_code} direction: {direction}, deal_weight: {deal_weight}, deal_price: {deal_price}")


with get_db_context_session() as session:
    data = session.query(StrategyDeal).filter(StrategyDeal.strategy_code == 'db_bt_hf1').all()
    data = list(map(lambda x: x.to_dict(), data))
    print(data)
    session.query(StrategyDeal).filter(StrategyDeal.strategy_code == 'db_bt_hf1').delete()
    session.commit()
    logger.info(f'删除数据成功')


def get_data_by_strategy(strategy_code='db_bt_hf1'):
    with get_db_context_session() as session:
        data = session.query(StrategyDeal).filter(StrategyDeal.strategy_code == strategy_code).all()
        data = list(map(lambda x: x.to_dict(), data))
        return data
