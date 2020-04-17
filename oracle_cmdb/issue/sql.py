# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleOnlineSQLTextIssue",
    "OracleOnlineSQLPlanIssue",
    "OracleOnlineSQLStatIssue",
]

from mongoengine import StringField, IntField

import rule.const
from .base import *


class OracleOnlineSQLIssue(OracleOnlineIssue):
    """oracle线上审核sql问题"""

    sql_id = StringField(required=True)

    meta = {
        "indexes": [
            "sql_id"
        ]
    }

    ENTRIES = (rule.const.RULE_ENTRY_ONLINE_SQL,)

    @classmethod
    def process(cls, collected=None, **kwargs):
        task_record_id: int = kwargs["task_record_id"]
        cmdb_id: int = kwargs["cmdb_id"]

        return


@OracleOnlineSQLIssue.need_collect()
class OracleOnlineSQLTextIssue(OracleOnlineSQLIssue):
    """oracle线上审核sql文本问题"""

    ENTRIES = (rule.const.RULE_ENTRY_ONLINE_SQL_TEXT,)


class OracleOnlineSQLExecutionIssue(OracleOnlineSQLIssue):
    """oracle线上审核sql运行问题"""

    plan_hash_value = IntField(required=True)


@OracleOnlineSQLIssue.need_collect()
class OracleOnlineSQLPlanIssue(OracleOnlineSQLExecutionIssue):
    """oracle线上审核sql执行计划问题"""

    ENTRIES = (rule.const.RULE_ENTRY_ONLINE_SQL_PLAN,)


@OracleOnlineSQLIssue.need_collect()
class OracleOnlineSQLStatIssue(OracleOnlineSQLExecutionIssue):
    """oracle线上审核sql执行信息问题"""

    ENTRIES = (rule.const.RULE_ENTRY_ONLINE_SQL_PLAN,)
