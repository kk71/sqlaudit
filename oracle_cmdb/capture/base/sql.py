# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "SQLCapturingDoc",
    "TwoDaysSQLCapturingDoc"
]

from datetime import datetime
from typing import NoReturn

import arrow
from mongoengine import IntField, StringField

import core.capture
from utils.log_utils import *
from models.mongoengine import BaseDoc, ABCTopLevelDocumentMetaclass
from oracle_cmdb import exceptions, const
from oracle_cmdb.plain_db import OraclePlainConnector
from utils.datetime_utils import dt_to_str


class SQLCapturingDoc(
    BaseDoc,
    core.capture.BaseCaptureItem,
    metaclass=ABCTopLevelDocumentMetaclass):
    """采集sql数据"""

    cmdb_id = IntField(required=True)
    task_record_id = IntField(required=True)

    meta = {
        'abstract': True,
        "indexes": [
            "cmdb_id",
            "task_record_id",
            "schema_name"
        ]
    }

    MODELS = []

    # select_workload_repository的参数
    PARAMETERS = (
        "elapsed_time",
        "cpu_time",
        "disk_reads",
        "executions",
        "buffer_gets"
    )

    @classmethod
    def query_snap_id(
            cls,
            cmdb_connector: OraclePlainConnector,
            start_time: datetime,
            end_time: datetime) -> (int, int):
        """获取可用的snapshot id"""
        sql = f"""
        SELECT min(snap_id),
        max(snap_id)
        FROM DBA_HIST_SNAPSHOT
        WHERE instance_number=1
          AND (end_interval_time BETWEEN
                to_date('{dt_to_str(start_time)}','yyyy-mm-dd hh24:mi:ss')
                AND to_date('{dt_to_str(end_time)}','yyyy-mm-dd hh24:mi:ss'))"""
        ret: [tuple] = cmdb_connector.select(sql)
        if not ret or len(ret) != 1 or len(ret[0]) != 2:
            raise exceptions.OracleSQLInvalidSnapId(f"{ret=}")
        s, e = ret[0][0], ret[0][1]
        if not s or not e or s == "None":
            raise exceptions.OracleSQLInvalidSnapId(f"{s=}, {e=}")
        try:
            s, e = int(s), int(e)
        except Exception:
            raise exceptions.OracleSQLInvalidSnapId(
                f"integer convert failed: {s=}, {e=}")
        if s == e:
            s = e - 1
        print(f"snap_id between {s} and {e}")
        return s, e

    @classmethod
    def query_snap_id_today(
            cls,
            cmdb_connector: OraclePlainConnector,
            now: arrow) -> (int, int):
        """查询今日从0点至今的snap_id"""
        return cls.query_snap_id(
            cmdb_connector,
            start_time=now.date(),
            end_time=now
        )

    @classmethod
    def query_snap_id_yesterday(
            cls,
            cmdb_connector: OraclePlainConnector,
            now: arrow) -> (int, int):
        """查询昨日0点至今日0点的snap_id"""
        return cls.query_snap_id(
            cmdb_connector,
            start_time=now.shift(days=-1).date(),
            end_time=now.date()
        )

    @classmethod
    def query_sql_set(
            cls,
            cmdb_connector: OraclePlainConnector,
            schema_name: str,
            beg_snap: int,
            end_snap: int) -> {(str, int, str)}:
        """查询sql基本信息，按照cls.PARAMETERS的参数分别查，最后去重"""
        ret = set()
        for parameter in cls.PARAMETERS:
            sql_info = cmdb_connector.select(f"""
SELECT sql_id,
plan_hash_value,
parsing_schema_name
FROM table(dbms_sqltune.select_workload_repository({beg_snap}, {end_snap},
            'parsing_schema_name=''{schema_name}''', NULL, '{parameter}', NULL,
            NULL, NULL, NULL))""")
            for sql_id, plan_hash_value, parsing_schema_name in sql_info:
                ret.add((str(sql_id), int(plan_hash_value), str(parsing_schema_name)))
        return ret

    @classmethod
    def post_captured(cls, **kwargs) -> NoReturn:
        docs: [cls] = kwargs["docs"]
        cmdb_id: int = kwargs["cmdb_id"]
        task_record_id: int = kwargs["task_record_id"]

        for doc in docs:
            doc.cmdb_id = cmdb_id
            doc.task_record_id = task_record_id

    @classmethod
    def _schema_sql_capture(cls,
                            a_schema: str,
                            snap_id_s: int,
                            snap_id_e: int,
                            m: "SQLCapturingDoc",
                            **kwargs) -> int:
        """
        单个schema的sql采集
        :param a_schema:
        :param snap_id_s:
        :param snap_id_e:
        :param m: 采集的model
        :param kwargs:
        :return: 采集到的sql数量
        """
        cmdb_connector: OraclePlainConnector = kwargs["cmdb_connector"]

        docs = []
        sql_set = cls.query_sql_set(
            cmdb_connector,
            schema_name=a_schema,
            beg_snap=snap_id_s,
            end_snap=snap_id_e
        )
        for sql_id, plan_hash_value, parsing_schema_name in sql_set:
            sql_to_run = m.simple_capture(
                sql_id=sql_id,
                plan_hash_value=plan_hash_value,
                schema_name=parsing_schema_name
            )
            docs += [
                m(**c)
                for c in cmdb_connector.select_dict(sql_to_run)
            ]
            m.post_captured(
                docs=docs,
                **kwargs
            )
        if not docs:
            return 0
        docs_inserted = m.objects.insert(docs)
        return len(docs_inserted)

    @classmethod
    def capture(cls, model_to_capture: ["SQLCapturingDoc"] = None, **kwargs):
        if model_to_capture is None:
            model_to_capture = cls.MODELS
        cmdb_id: int = kwargs["cmdb_id"]
        task_record_id: int = kwargs["task_record_id"]
        cmdb_conn: OraclePlainConnector = kwargs["cmdb_connector"]
        schemas: [str] = kwargs["schemas"]
        now: arrow = kwargs["now"]

        snap_id_s, snap_id_e = cls.query_snap_id_today(cmdb_conn, now)

        for i, m in enumerate(model_to_capture):
            i += 1
            total = len(model_to_capture)
            print(f"* running {i} of {total}: {m.__doc__}")
            with schema_no_data(m.__doc__) as schema_counter:
                for a_schema in schemas:
                    captured_num = cls._schema_sql_capture(
                        a_schema=a_schema,
                        snap_id_s=snap_id_s,
                        snap_id_e=snap_id_e,
                        cmdb_conn=cmdb_conn,
                        m=m,
                        cmdb_id=cmdb_id,
                        task_record_id=task_record_id,
                        cmdb_connector=cmdb_conn
                    )
                    schema_counter(a_schema, captured_num)


class TwoDaysSQLCapturingDoc(SQLCapturingDoc):
    """需要采集两天的sql数据"""

    two_days_capture = StringField(choices=const.ALL_TWO_DAYS_CAPTURE)

    meta = {
        'abstract': True,
        "indexes": [
            "two_days_capture"
        ]
    }

    MODELS = []

    @classmethod
    def post_captured(cls, **kwargs) -> NoReturn:
        docs: [cls] = kwargs["docs"]
        two_days_capture: str = kwargs["two_days_capture"]

        SQLCapturingDoc.post_captured(**kwargs)
        for doc in docs:
            doc.two_days_capture = two_days_capture

    @classmethod
    def capture(cls, model_to_capture: ["TwoDaysSQLCapturingDoc"] = None, **kwargs):
        if model_to_capture is None:
            model_to_capture = cls.MODELS
        cmdb_id: int = kwargs["cmdb_id"]
        task_record_id: int = kwargs["task_record_id"]
        cmdb_conn: OraclePlainConnector = kwargs["cmdb_connector"]
        schemas: [str] = kwargs["schemas"]
        now: arrow = kwargs["now"]

        today_snap_id_s, today_snap_id_e = cls.query_snap_id_today(
            cmdb_conn, now)
        yesterday_snap_id_s, yesterday_snap_id_e = cls.query_snap_id_yesterday(
            cmdb_conn, now)

        for i, m in enumerate(model_to_capture):
            i += 1
            total = len(model_to_capture)
            print(f"* running {i} of {total}: {m.__doc__}")
            with schema_no_data(
                    f"{m.__doc__}-today",
                    show_lable_in_schema_print=True
            ) as schema_counter_today:
                with schema_no_data(
                        f"{m.__doc__}-yesterday",
                        show_lable_in_schema_print=True
                ) as schema_counter_yesterday:
                    for a_schema in schemas:
                        today_captured = cls._schema_sql_capture(
                            a_schema=a_schema,
                            snap_id_s=today_snap_id_s,
                            snap_id_e=today_snap_id_e,
                            cmdb_conn=cmdb_conn,
                            m=m,
                            cmdb_id=cmdb_id,
                            task_record_id=task_record_id,
                            cmdb_connector=cmdb_conn,
                            two_days_capture=const.SQL_TWO_DAYS_CAPTURE_TODAY
                        )
                        schema_counter_today(a_schema, today_captured)
                        yesterday_captured = cls._schema_sql_capture(
                            a_schema=a_schema,
                            snap_id_s=yesterday_snap_id_s,
                            snap_id_e=yesterday_snap_id_e,
                            cmdb_conn=cmdb_conn,
                            m=m,
                            cmdb_id=cmdb_id,
                            task_record_id=task_record_id,
                            cmdb_connector=cmdb_conn,
                            two_days_capture=const.SQL_TWO_DAYS_CAPTURE_YESTERDAY
                        )
                        schema_counter_yesterday(a_schema, yesterday_captured)
