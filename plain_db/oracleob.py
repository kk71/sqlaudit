# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "DBError",
    "OracleOB",
    "OracleCMDBConnector"
]


import os
from collections import OrderedDict

import cx_Oracle

import settings
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

from models.oracle import CMDB


class DBError(Exception):
    pass


class OracleHelper:

    pool = None

    @classmethod
    def get_conn(cls):
        if cls.pool is None:
            cls.dsn = cx_Oracle.makedsn(settings.ORACLE_IP, str(settings.ORACLE_PORT), settings.ORACLE_SID)
            cls.pool = cx_Oracle.SessionPool(
                settings.ORACLE_USERNAME,
                settings.ORACLE_PASSWORD,
                cls.dsn,
                settings.ORACLE_MIN_CONN,
                settings.ORACLE_MAX_CONN,
                settings.ORACLE_INCREMENT,
                # timeout=5,
                # waitTimeout=5
            )
        return cls.pool.acquire()

        # conn = cx_Oracle.connect('loge/china@192.168.1.11:1562/prod')
        # self.handle = "%s:%s/%s" % (self.host, str(self.port), self.service_name)
        # self.conn = cx_Oracle.connect(
        #     self.username, self.password, self.handle)

    @classmethod
    def insert(cls, sql, params=None):
        params = params or []
        try:
            # print(sql, params)
            conn = cls.get_conn()
            cursor = conn.cursor()
            cursor.execute(sql, params)
        except cx_Oracle.OperationalError as e:
            print(str(e))
            conn.rollback()
            return str(e)
        finally:
            conn.commit()
            cursor.close()
            cls.pool.release(conn)

    @classmethod
    def update(cls, sql, params=None):
        return cls.insert(sql, params)

    @classmethod
    def select_dict(cls, sql, params=None, one=True):
        params = params or []
        conn = cls.get_conn()
        cursor = conn.cursor()
        cursor.execute(sql, params)
        fields = [x[0].lower() for x in cursor.description]
        data = (cursor.fetchone() or []) if one else cursor.fetchall()
        cursor.close()
        cls.pool.release(conn)

        data = data or ()
        if isinstance(data, tuple):
            return OrderedDict(zip(fields, data))

        # Else is a list, fetchall()
        # return [dict(zip(fields, item)) for item in data]
        return [OrderedDict(zip(fields, item)) for item in data]

    @classmethod
    def executemany(cls, sql, params):
        params = params or [[]]
        try:
            conn = cls.get_conn()
            cursor = conn.cursor()
            cursor.executemany(sql, params)
        except cx_Oracle.OperationalError as e:
            print(str(e))
            conn.rollback()
        finally:
            conn.commit()
            cursor.close()
            cls.pool.release(conn)

    @classmethod
    def select(cls, sql, params=None, one=True):
        params = params or []
        conn = cls.get_conn()
        cursor = conn.cursor()
        cursor.execute(sql, params)
        res = [x for x in (cursor.fetchone() or [])] if one else [list(x) for x in cursor.fetchall()]
        cursor.close()
        cls.pool.release(conn)
        return res

    @classmethod
    def select_with_lob(cls, sql, params=None, one=True, index=(2)):
        params = params or []
        conn = cls.get_conn()
        cursor = conn.cursor()
        cursor.execute(sql, params)
        if one:
            res = [(x.read() if i in index else x) for i, x in enumerate(cursor.fetchone() or [])]
        else:
            res = [[(x.read() if x and i in index else x) for i, x in enumerate(row)] for row in cursor.fetchall()]
        cursor.close()
        cls.pool.release(conn)
        return res

    @classmethod
    def select_with_lob_dict(cls, sql, params=None, one=True, index=2):
        params = params or []
        conn = cls.get_conn()
        cursor = conn.cursor()
        cursor.execute(sql, params)
        if one:
            data = [(x.read() if i == index else x) for i, x in enumerate(cursor.fetchone() or ())]
        else:
            data = [[(x.read() if i == index else x) for i, x in enumerate(row)] for row in cursor.fetchall() or []]
        fields = [x[0].lower() for x in cursor.description]

        if isinstance(data, tuple):
            return dict(zip(fields, data))

        return [dict(zip(fields, item)) for item in data]

        cursor.close()
        cls.pool.release(conn)

    @classmethod
    def delete(cls, sql, params=None):
        return cls.insert(sql, params)

    @classmethod
    def execute(cls, sql, params=None):
        params = params or []
        conn = cls.get_conn()
        cursor = conn.cursor()
        cursor.execute(sql, params)
        conn.commit()
        cursor.close()
        cls.pool.release(conn)


class OracleOB:

    def __init__(self, host, port, username, password, sid=None, service_name=None, charset="utf8"):

        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.sid = sid
        self.service_name = service_name

        self.dsn = cx_Oracle.makedsn(self.host, str(self.port), self.sid)
        self.conn = cx_Oracle.connect(self.username, self.password, self.dsn)
        self.cursor = self.conn.cursor()

        # conn = cx_Oracle.connect('loge/china@192.168.1.11:1562/prod')
        # self.handle = "%s:%s/%s" % (self.host, str(self.port), self.service_name)
        # self.conn = cx_Oracle.connect(
        #     self.username, self.password, self.handle)

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

    def select_dict(self, sql, params=None, one=True):
        params = params or []
        self.cursor.execute(sql, params)
        fields = [x[0].lower() for x in self.cursor.description]
        data = (self.cursor.fetchone() or ()) if one else self.cursor.fetchall()

        data = data or ()
        if isinstance(data, tuple):
            return dict(zip(fields, data))

        # Else is a list, fetchall()
        return [dict(zip(fields, item)) for item in data]

    def select(self, sql, params=None, one=True):
        params = params or []
        self.cursor.execute(sql, params)
        return (self.cursor.fetchone() or ()) if one else self.cursor.fetchall()

    def delete(self, sql, params=None):
        self.insert(sql, params)

    def close(self):
        self.cursor.close()
        self.conn.close()


class OracleCMDBConnector(OracleOB):
    """
    用于快速连接oracle纳管库
    """
    def __init__(self, cmdb: CMDB):
        super(OracleCMDBConnector, self).__init__(
            host=cmdb.ip_address,
            port=cmdb.port,
            username=cmdb.user_name,
            password=cmdb.password,
            sid=cmdb.service_name,  # TODO
            service_name=cmdb.sid  # TODO
        )
