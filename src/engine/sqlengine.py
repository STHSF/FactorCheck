#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""
@version: ??
@author: li
@file: sqlengine.py
@time: 2019/11/7 10:00 上午
"""
import pandas as pd
import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker


class SqlEngine(object):

    def __init__(self, destination_db):
        self.con = sa.create_engine(destination_db)
        self.session = self.create_session()

    def __del__(self):
        if self.session:
            self.session.close()

    def create_session(self):
        dest_session = sessionmaker(bind=self.con, autocommit=False, autoflush=True)
        return dest_session()

    def fetch_data(self, table_name):
        df_list = []
        for chunk in pd.read_sql(table_name, self.con, chunksize=1000000):
            df_list.append(chunk)
        res = pd.concat(df_list, ignore_index=True)
        return res