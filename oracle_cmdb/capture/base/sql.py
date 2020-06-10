# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleSQLCapturingDoc",
    "OracleTwoDaysSQLCapturingDoc"
]

from datetime import datetime
from typing import NoReturn, List, Union, Tuple
from collections import defaultdict

import arrow
from mongoengine import IntField, StringField

import settings
from .base import *
from utils.log_utils import *
from models.mongoengine import *
from oracle_cmdb import exceptions, const
from oracle_cmdb.plain_db import OraclePlainConnector
from utils.datetime_utils import dt_to_str


class OracleSQLCapturingDoc(
        BaseDoc,
        BaseOracleCapture,
        metaclass=SelfCollectingTopLevelDocumentMetaclass):
    """采集sql数据"""

    cmdb_id = IntField(required=True)
    schema_name = StringField(required=True)
    task_record_id = IntField(required=True)

    meta = {
        'abstract': True,
        "indexes": [
            "cmdb_id",
            "task_record_id",
            "schema_name"
        ]
    }

    COLLECTED = []

    # 批量采集按照sql_id作为单位，一次SQL查询传入的sql_id数
    SQL_ID_BULK_NUM = settings.ORACLE_SQL_ID_BULK_NUM

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

    @staticmethod
    def list_to_oracle_str(l: List[Union[str, int, float]]):
        """一个list转换为oracle的数组"""
        inner = ""
        if l:
            inner = ", ".join([f"'{str(i)}'" for i in l])
        return f"({inner})"

    @classmethod
    def convert_sql_set_bulk_to_sql_filter(cls, sql_set_bulk):
        """把sql set bulk转换为sql查询语句"""
        raise NotImplementedError

    @classmethod
    def query_sql_set(
            cls,
            cmdb_connector: OraclePlainConnector,
            schema_name: str,
            beg_snap: int,
            end_snap: int) -> List[Tuple[str, set]]:
        """查询sql基本信息，按照cls.PARAMETERS的参数分别查，最后去重"""
        ret_dict = defaultdict(set)
        for parameter in cls.PARAMETERS:
            sql_info = cmdb_connector.select(f"""
SELECT sql_id,
plan_hash_value,
parsing_schema_name
FROM table(dbms_sqltune.select_workload_repository({beg_snap}, {end_snap},
            'parsing_schema_name=''{schema_name}''', NULL, '{parameter}', NULL,
            NULL, NULL, NULL))""")
            for sql_id, plan_hash_value, parsing_schema_name in sql_info:
                ret_dict[str(sql_id)].add(int(plan_hash_value))
        return list(ret_dict.items())

    @classmethod
    def query_sql_set_bulk(cls, *args, **kwargs):
        """批量返回sql_set"""
        sql_set = cls.query_sql_set(*args, **kwargs)
        while sql_set:
            yield sql_set[-cls.SQL_ID_BULK_NUM:]
            del sql_set[-cls.SQL_ID_BULK_NUM:]

    @classmethod
    def post_captured(cls, **kwargs) -> NoReturn:
        docs: [cls] = kwargs["docs"]
        cmdb_id: int = kwargs["cmdb_id"]
        schema_name: str = kwargs["schema_name"]
        task_record_id: int = kwargs["task_record_id"]

        for doc in docs:
            doc.cmdb_id = cmdb_id
            doc.task_record_id = task_record_id
            doc.schema_name = schema_name

    @classmethod
    def _schema_sql_capture(cls,
                            a_schema: str,
                            m: "OracleSQLCapturingDoc",
                            **kwargs) -> int:
        """
        单个schema的sql采集
        :param a_schema:
        :param snap_id_s:
        :param snap_id_e:
        :param m: 采集的model
        :param kwargs:
        :return: 采集到sql的相应数据量
        """
        cmdb_connector: OraclePlainConnector = kwargs["cmdb_connector"]
        snap_id_s: int = kwargs["snap_id_s"]
        snap_id_e: int = kwargs["snap_id_e"]

        docs = []
        sql_set_generator = cls.query_sql_set_bulk(
            cmdb_connector,
            schema_name=a_schema,
            beg_snap=snap_id_s,
            end_snap=snap_id_e
        )
        for sql_set_bulk in sql_set_generator:
            sql_to_run = m.simple_capture(
                schema_name=a_schema,
                snap_id_s=snap_id_s,
                snap_id_e=snap_id_e,
                filters=m.convert_sql_set_bulk_to_sql_filter(sql_set_bulk)
            )
            docs += [
                m(**c)
                for c in cmdb_connector.select_dict(sql_to_run)
            ]
            m.post_captured(
                docs=docs,
                schema_name=a_schema,
                **kwargs
            )
        if not docs:
            return 0
        docs_inserted = m.insert(docs)
        return len(docs_inserted)

    @classmethod
    def process(cls, collected: ["OracleSQLCapturingDoc"] = None, **kwargs):

        from oracle_cmdb.tasks.capture.cmdb_task_stats import \
            OracleCMDBTaskStatsSnapIDPairs

        if collected is None:
            collected = cls.COLLECTED
        cmdb_id: int = kwargs["cmdb_id"]
        task_record_id: int = kwargs["task_record_id"]
        cmdb_conn: OraclePlainConnector = kwargs["cmdb_connector"]
        schemas: [str] = kwargs["schemas"]
        now: arrow = kwargs["now"]

        snap_id_s, snap_id_e = cls.query_snap_id_today(cmdb_conn, now)
        OracleCMDBTaskStatsSnapIDPairs.write_stats(
            # 记录当日采集的snap shot id pairs
            task_record_id,
            cls,
            snap_shot_id_pair=(snap_id_s, snap_id_e)
        )

        for i, m in enumerate(collected):
            i += 1
            total = len(collected)
            print(f"* running {i} of {total}: {m.__doc__}")
            with grouped_count_logger(
                    m.__doc__, item_type_name="schema") as schema_counter:
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


class OracleTwoDaysSQLCapturingDoc(OracleSQLCapturingDoc):
    """需要采集两天的sql数据"""

    two_days_capture = StringField(choices=const.ALL_TWO_DAYS_CAPTURE)

    meta = {
        'abstract': True,
        "indexes": [
            "two_days_capture"
        ]
    }

    COLLECTED = []

    @classmethod
    def post_captured(cls, **kwargs) -> NoReturn:
        docs: [cls] = kwargs["docs"]
        two_days_capture: str = kwargs["two_days_capture"]

        OracleSQLCapturingDoc.post_captured(**kwargs)
        for doc in docs:
            doc.two_days_capture = two_days_capture

    @classmethod
    def process(cls, collected: ["OracleTwoDaysSQLCapturingDoc"] = None, **kwargs):
        if collected is None:
            collected = cls.COLLECTED
        cmdb_id: int = kwargs["cmdb_id"]
        task_record_id: int = kwargs["task_record_id"]
        cmdb_conn: OraclePlainConnector = kwargs["cmdb_connector"]
        schemas: [str] = kwargs["schemas"]
        now: arrow = kwargs["now"]

        today_snap_id_s, today_snap_id_e = cls.query_snap_id_today(
            cmdb_conn, now)
        yesterday_snap_id_s, yesterday_snap_id_e = cls.query_snap_id_yesterday(
            cmdb_conn, now)

        for i, m in enumerate(collected):
            i += 1
            total = len(collected)
            print(f"* running {i} of {total}: {m.__doc__}")
            with grouped_count_logger(
                    f"{m.__doc__}-today",
                    item_type_name="schema",
                    show_label_in_print=True
            ) as schema_counter_today:
                with grouped_count_logger(
                        f"{m.__doc__}-yesterday",
                        item_type_name="schema",
                        show_label_in_print=True
                ) as schema_counter_yesterday:
                    for a_schema in schemas:
                        common_params = dict(
                            a_schema=a_schema,
                            cmdb_conn=cmdb_conn,
                            m=m,
                            cmdb_id=cmdb_id,
                            task_record_id=task_record_id,
                            cmdb_connector=cmdb_conn,
                        )
                        today_captured = cls._schema_sql_capture(
                            **common_params,
                            snap_id_s=today_snap_id_s,
                            snap_id_e=today_snap_id_e,
                            two_days_capture=const.SQL_TWO_DAYS_CAPTURE_TODAY
                        )
                        schema_counter_today(a_schema, today_captured)
                        yesterday_captured = cls._schema_sql_capture(
                            **common_params,
                            snap_id_s=yesterday_snap_id_s,
                            snap_id_e=yesterday_snap_id_e,
                            two_days_capture=const.SQL_TWO_DAYS_CAPTURE_YESTERDAY
                        )
                        schema_counter_yesterday(a_schema, yesterday_captured)
