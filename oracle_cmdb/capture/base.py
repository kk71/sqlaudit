# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "ObjectCapturingDoc",
    "SchemaObjectCapturingDoc",
    "SQLCapturingDoc"
]

from typing import NoReturn

import core.capture
from mongoengine import IntField, StringField, ObjectIdField

from models.mongoengine import *
from utils.datetime_utils import *
from ..plain_db import OraclePlainConnector


class ObjectCapturingDoc(
        BaseDoc,
        core.capture.BaseCaptureItem,
        metaclass=ABCTopLevelDocumentMetaclass):
    """对象采集"""

    _id = ObjectIdField()
    cmdb_id = IntField()
    task_record_id = IntField(required=True)

    meta = {
        'abstract': True,
        "indexes": [
            "cmdb_id",
            "task_record_id"
        ]
    }

    @classmethod
    def post_captured(cls, **kwargs) -> NoReturn:
        docs: ["ObjectCapturingDoc"] = kwargs["docs"]
        cmdb_id: int = kwargs["cmdb_id"]
        task_record_id: int = kwargs["task_record_id"]

        for d in docs:
            d.from_dict({
                "cmdb_id": cmdb_id,
                "task_record_id": task_record_id,
            })


class SchemaObjectCapturingDoc(ObjectCapturingDoc):
    """面向schema的对象采集"""

    schema_name = StringField(required=True)

    meta = {
        'abstract': True,
        "indexes": [
            "schema_name"
        ]
    }

    @classmethod
    def post_captured(cls, **kwargs) -> NoReturn:
        docs: ["SchemaObjectCapturingDoc"] = kwargs["docs"]
        cmdb_id: int = kwargs["cmdb_id"]
        task_record_id: int = kwargs["task_record_id"]
        obj_owner: str = kwargs.pop["obj_owner"]

        for d in docs:
            d.from_dict({
                "cmdb_id": cmdb_id,
                "schema_name": obj_owner,
                "task_record_id": task_record_id,
            })


class SQLCapturingDoc(BaseDoc):
    """sql采集"""

    _id = ObjectIdField()
    cmdb_id = IntField()
    task_record_id = IntField(required=True)

    meta = {
        'abstract': True,
        "indexes": [
            "cmdb_id",
            "task_record_id",
            "schema_name"
        ]
    }

    @classmethod
    def query_snap_id(
            cls,
            cmdb_connector: OraclePlainConnector,
            start_time: datetime,
            end_time: datetime) -> (int, int):
        """
        获取可用的snap_id
        :param cmdb_connector:
        :param start_time:
        :param end_time:
        :return:
        """
        sql = f"""
        SELECT min(snap_id),
        max(snap_id)
        FROM DBA_HIST_SNAPSHOT
        WHERE instance_number=1
          AND (end_interval_time BETWEEN
                to_date('{dt_to_str(start_time)}','yyyy-mm-dd hh24:mi:ss')
                AND to_date('{dt_to_str(end_time)}','yyyy-mm-dd hh24:mi:ss'))"""
        ret = cmdb_connector.execute(sql)
        return ret[0][0], ret[0][1]

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
            start_time=now.shift(days=-1).date,
            end_time=now.date()
        )

