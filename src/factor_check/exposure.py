#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""
@version: ??
@author: li
@file: exposure.py
@time: 2019/11/7 10:38 上午
"""
import sys
sys.path.append('../')
sys.path.append('../../')
sys.path.append('../../../')
import pandas as pd
from src.engine import sqlengine
from joblib import Parallel, delayed
from src import config
from src.utills import log_util
import multiprocessing
import sqlite3 as lite


class Exposure(object):

    def __init__(self, table_name):
        destination_db = '''mysql+mysqlconnector://{0}:{1}@{2}:{3}/{4}'''.format(config.rl_db_user,
                                                                                 config.rl_db_pwd,
                                                                                 config.rl_db_host,
                                                                                 config.rl_db_port,
                                                                                 config.rl_db_database)
        self.engine = sqlengine.SqlEngine(destination_db)
        self.factor_data: pd.DataFrame() = self.engine.fetch_data(table_name)
        print(len(self.factor_data))
        # self.factor_data = pd.DataFrame({'trade_date': [1, 1, 2, 2, 2, 3, 3, 4],
        #                                  'b': [2, 3, None, 5, None, None, 7, 8],
        #                                  'c': [None, 2, 3, 4, 5, 6, 7, 8]})

    @staticmethod
    def non_rate(df_data):
        tmp = pd.DataFrame(df_data.isnull().sum()).T
        tmp = tmp.apply(lambda x: x / len(df_data), axis=1)
        tmp['trade_date'] = df_data['trade_date'].values[0]
        return tmp

    @staticmethod
    def apply_parallel(df_grouped, func):
        ret_lst = Parallel(n_jobs=int(multiprocessing.cpu_count() / 2))(delayed(func)(group) for name, group in df_grouped)
        return pd.concat(ret_lst, ignore_index=True)

    def calculate(self):
        self.factor_data['trade_date'] = self.factor_data['trade_date'].apply(lambda x: x.strftime('%Y-%m-%d'))
        # result = self.factor_data.groupby('trade_date').apply(lambda x: x.isnull().sum())
        multi_res = self.apply_parallel(self.factor_data.groupby('trade_date'), self.non_rate).set_index('trade_date')
        # multi_res = multi_res.apply(lambda x: x/len(self.factor_data), axis=1)
        return multi_res


if __name__ == '__main__':
    log = log_util.Logger('exposure', level='info')
    tb_list = ['factor_earning', 'factor_cash_flow', 'factor_capital_structure', 'factor_historical_growth',
               'factor_operation_capacity', 'factor_per_share_indicators', 'factor_revenue_quality', 'factor_solvency',
               'factor_basic_derivation', 'factor_earning_expectation']
    tb = tb_list[1]

    log.logger.info('exposure of {}'.format(tb))
    exposure = Exposure(table_name=tb)
    result = exposure.calculate()
    log.logger.info('len of result: {}'.format(len(result)))
    log.logger.info(result.reset_index())
    with lite.connect('./data.db') as db:
        result.reset_index().to_sql(name=tb, con=db, if_exists='replace', index=None)
    log.logger.info('calculate finish')
    log.logger.info('==============================================================')
