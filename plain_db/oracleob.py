# -*- coding: utf-8 -*-

import os
from collections import OrderedDict
import cx_Oracle
import settings
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'


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

if __name__ == "__main__":
    pass

    odb = OracleOB("114.115.135.118", "1521", "isqlaudit", "v1g2m60id2499yz", "ora12c01")
    # print(odb.select("SELECT * FROM T_USER WHERE login_user = 'admin'"))
    # statement_id = 'jmte3qn0cdtckbg'
    # sql = "Select 1 from dual"
    # sql = f"EXPLAIN PLAN SET statement_id='{statement_id}' for {sql}"
    # print(sql)
    sql = """
        INSERT INTO T_SQL_PLAN(work_list_id, statement_id, plan_id, timestamp, remarks, operation, options, object_node,
                               object_owner, object_name, object_alias, object_instance, object_type, optimizer, search_columns,
                               id, parent_id, depth, position, cost, cardinality, bytes, other_tag, partition_start,
                               partition_stop, partition_id, distribution, cpu_cost, io_cost, temp_space, access_predicates, filter_predicates, projection, time, qblock_name) VALUES(:1, :2, :3, to_date(:4, 'yyyy-mm-dd hh24:mi:ss'), :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17, :18, :19, :20, :21, :22, :23, :24, :25, :26, :27, :28, :29, :30, :31, :32, :33, :34, :35)
    """
    b = """work_list_id, "statement_id", plan_id, "timestamp", remarks, operation, options, object_node,
                               object_owner, object_name, object_alias, object_instance, object_type, optimizer, search_columns,
                               "id", parent_id, depth, position, "cost", "cardinality", bytes, other_tag, partition_start,
                               partition_stop, partition_id, distribution, cpu_cost, io_cost, temp_space, access_predicates, filter_predicates, projection, time, qblock_name"""
    # print(len(b.split(',')))
    # # zip(b.split(','), params)
    # # a = datetime.datetime(2018, 5, 27, 4, 33, 2)
    # params = [743, '0qz9qcfizickzj3', 283, '2018-05-12 00:00:00', '', 'SELECT STATEMENT', '', '',
    #           '', '', '', '', '', 'ALL_ROWS', '', 0, '', 0, 1, 1, 2, 8, '', '', '', '', '', 7521, 1, '', '', '',
    #           '', 1, '']
    # for x in list(zip(b.split(','), params)):
    #     print(x)
    # print(list(zip(b.split(','), params)))
    # print(len(params))
    # print(odb.insert(sql, params))
    # print(odb.select_dict("SELECT * FROM T_USER WHERE login_user = 'admin'"))
    # sql = "INSERT INTO T_USER(login_user, user_name, password, email, mobile_phone, create_date) VALUES(:1, :2, :3, :4, :5, to_date(:6, 'yyyy-MM-dd HH24:mi:ss'))"
    # sql = "SELECT SEQ_CMDB.NEXTVAL FROM T_CMDB "
    # sql = "SELECT * FROM T_USER WHERE login_user = 'admin'"
    # sql = "update t_user set create_date = to_date('2013-02-18 21:57:00','yyyy-mm-dd hh24:mi:ss');"

    sql = "SELECT SEQ_MAIL_SERVER.nextval FROM MAIL_SERVER "
    b = odb.select(sql)
    print(b)
    # current_time = datetime.now()

    # sql = """UPDATE T_WORK_LIST SET work_list_status='{0}', audit_comments='{1}', audit_date='{2}'
    #          WHERE work_list_id = '{3}'
    # odb.update("UPDATE T_USER SET login_user = 'user_login' where login_user = 'login_user'")
    # odb.delete("DELETE FROM T_USER WHERE login_user = 'aaa'")
