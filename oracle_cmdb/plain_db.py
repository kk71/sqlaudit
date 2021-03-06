# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OraclePlainConnector"
]


import os
from typing import Union

import cx_Oracle

os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

import settings


class OraclePlainConnector:
    """oracle纳管库的连接器"""

    def __init__(self,
                 ip_address,
                 port,
                 username,
                 password,
                 **kwargs):
        self.dsn = cx_Oracle.makedsn(ip_address, str(port), **kwargs)
        self.conn = cx_Oracle.connect(username, password, self.dsn)
        self.cursor = self.conn.cursor()

    def modify(self, sql, params=None):
        """DML语句修改数据时请使用该方法"""
        params = params or []
        try:
            print(sql, params)
            self.execute(sql, params)
        except cx_Oracle.OperationalError as e:
            print(str(e))
            self.conn.rollback()
        self.conn.commit()

    def execute(self, sql, params=None):
        params = params or []
        try:
            self.cursor.execute(sql, params)
        except Exception as e:
            if settings.ECHO_SQL_WHEN_FAIL:
                print(f"""\n============================
Oracle CMDB sql execution failed at: 
{sql}
============================""")
            raise e

    def select_dict(self, sql, params=None, one=False) -> Union[list, dict]:
        """select语句返回字典形式，key全部小写"""
        params = params or []
        self.execute(sql, params)
        fields = [x[0].lower() for x in self.cursor.description]
        if one:
            data = self.cursor.fetchone() or ()
            return dict(zip(fields, data))
        else:
            data = self.cursor.fetchall()
            return [dict(zip(fields, item)) for item in data]

    def select(self, sql, params=None, one=False) -> [tuple]:
        """select语句返回列表元组形式"""
        params = params or []
        self.execute(sql, params)
        return self.cursor.fetchone() or () if one else self.cursor.fetchall()

    def close(self):
        self.cursor.close()
        self.conn.close()
