#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""
@version: ??
@author: li
@file: correlation.py
@time: 2019/11/7 10:49 上午
"""
import sys
sys.path.append('../')
sys.path.append('../../')
sys.path.append('../../../')
import pandas as pd
from src.engine.internal_code import *
from src import config
import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker

from src.utills import log_util
# pandas
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('max_colwidth',100)

class Correlation(object):

    def __init__(self):
        destination_db = '''mysql+mysqlconnector://{0}:{1}@{2}:{3}/{4}'''.format(config.rl_db_user,
                                                                                 config.rl_db_pwd,
                                                                                 config.rl_db_host,
                                                                                 config.rl_db_port,
                                                                                 config.rl_db_database)
        # 源数据库
        self.source = sa.create_engine(destination_db)
        # 数据库Session
        self.session = sessionmaker(bind=self.source, autocommit=False, autoflush=True)

    def get_data(self, fatcor, tabe_name, trade_date):
        sql = """select trade_date, security_code, `{0}` from `{1}` where trade_date='{2}' order by security_code """.format(fatcor, tabe_name, trade_date)
        result_list = pd.read_sql(sql, self.source)
        return result_list


if __name__ == '__main__':
    internal = InternalCode()
    cc = Correlation()
    factor_data = cc.get_data('LogofMktValue', 'factor_valuation_estimation', 20190930)
    print(factor_data.head())
    code = internal.get_Ashare_internal_code_list(['20190930'])
    print(code.sort_values('security_code').head())

    res = pd.merge(code, factor_data, on=['security_code', 'trade_date'])

    print(res.sort_values('security_code').head())

    excel = pd.read_csv('../data/因子比对.csv')

    excel['symbol'] = excel['symbol'].apply(lambda x: "{:06d}".format(x) + '.XSHG' if len(str(x)) == 6 and str(x)[0] in '6' else "{:06d}".format(x) + '.XSHE')

    print(excel.head())

    res1 = pd.merge(res, excel, on=['symbol'])
    res2 = res1.sort_values('symbol')[['symbol', 'LogofMktValue', 'WD_LogofMktValue']]

    res2['abs_diff'] = abs(res2['WD_LogofMktValue'] - res2['LogofMktValue'])
    res2.to_csv('../data/result.csv')
    print(res2.describe())

    print(res2[['WD_LogofMktValue', 'LogofMktValue']].corr())

