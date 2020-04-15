# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OraclePlainConnector"
]


import os
from typing import Union

import cx_Oracle

os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'


class OraclePlainConnector:

    def __init__(self,
                 ip_address,
                 port,
                 username,
                 password,
                 sid=None,
                 service_name=None):
        assert sid or service_name
        self.ip_address = ip_address
        self.port = port
        self.username = username
        self.password = password
        self.sid = sid
        self.service_name = service_name

        self.dsn = cx_Oracle.makedsn(self.ip_address, str(self.port), self.sid)
        self.conn = cx_Oracle.connect(self.username, self.password, self.dsn)
        self.cursor = self.conn.cursor()

    def insert(self, sql, params=None):
        params = params or []
        try:
            print(sql, params)
            self.cursor.execute(sql, params)
        except cx_Oracle.OperationalError as e:
            print(str(e))
            self.conn.rollback()
        self.conn.commit()

    def execute(self, sql, params=None):
        params = params or []
        self.cursor.execute(sql, params)

    def update(self, sql, params=None):
        self.insert(sql, params)

    def select_dict(self, sql, params=None, one=True) -> Union[list, dict]:
        params = params or []
        self.cursor.execute(sql, params)
        fields = [x[0].lower() for x in self.cursor.description]
        if one:
            data = self.cursor.fetchone() or ()
            return dict(zip(fields, data))
        else:
            data = self.cursor.fetchall()
            return [dict(zip(fields, item)) for item in data]

    def select(self, sql, params=None, one=True):
        params = params or []
        self.cursor.execute(sql, params)
        return self.cursor.fetchone() or () if one else self.cursor.fetchall()

    def delete(self, sql, params=None):
        self.insert(sql, params)

    def close(self):
        self.cursor.close()
        self.conn.close()
