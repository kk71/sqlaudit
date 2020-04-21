# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleOnlineSQLIssue"
]

from typing import NoReturn

from mongoengine import StringField

import rule.const
from .base import *
from models.mongoengine import *
from ..capture import SQLText


class OracleOnlineSQLIssue(OracleOnlineIssue):
    """sql问题"""

    sql_id = StringField(required=True, default=None)

    meta = {
        "allow_inheritance": True,
        "indexes": [
            "sql_id"
        ]
    }

    ENTRIES = (rule.const.RULE_ENTRY_ONLINE_SQL,)

    @classmethod
    def post_analysed(cls, **kwargs) -> NoReturn:
        docs: ["OracleOnlineIssue"] = kwargs["docs"]
        sql_id: str = kwargs["sql_id"]

        super().post_analysed(**kwargs)
        for doc in docs:
            doc.sql_id = sql_id

    @classmethod
    def get_sql_qs(
            cls,
            task_record_id: int,
            schema_name: str) -> mongoengine_qs:
        """获取采集任务的sql_id"""
        return SQLText.objects(
            task_record_id=task_record_id,
            schema_name=schema_name
        )


