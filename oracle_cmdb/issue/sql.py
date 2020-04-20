# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleOnlineSQLTextIssue",
    "OracleOnlineSQLPlanIssue",
    "OracleOnlineSQLStatIssue",
]

from typing import NoReturn, Generator

from mongoengine import StringField, IntField

import rule.const
from utils.log_utils import *
from .base import *
from rule.rule import CMDBRule
from ..cmdb import OracleCMDB
from models.sqlalchemy import *
from ..adapters import *
from ..capture import SQLText, SQLPlan, SQLStat


class OracleOnlineSQLIssue(OracleOnlineIssue):
    """oracle线上审核sql问题"""

    sql_id = StringField(required=True, default=None)

    meta = {
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
    def get_sql(cls, task_record_id: int) -> [SQLText]:
        """获取采集任务的sql_id"""
        return list(SQLText.objects(task_record_id=task_record_id))


class OracleOnlineSQLExecutionIssue(OracleOnlineSQLIssue):
    """oracle线上审核sql运行问题"""

    plan_hash_value = IntField(required=True, default=None)

    @classmethod
    def post_analysed(cls, **kwargs) -> NoReturn:
        docs: ["OracleOnlineSQLExecutionIssue"] = kwargs["docs"]
        plan_hash_value: int = kwargs["plan_hash_value"]

        super().post_analysed(**kwargs)
        for doc in docs:
            doc.plan_hash_value = plan_hash_value

    @classmethod
    def get_sql_plan(cls,
                     task_record_id: int,
                     sql_id: str = None,
                     plan_hash_value: int = None):
        pass

    @classmethod
    def get_sql_stat(cls,
                     task_record_id: int,
                     sql_id: str = None,
                     plan_hash_value: int = None):
        pass


@OracleOnlineIssue.need_collect()
class OracleOnlineSQLTextIssue(OracleOnlineSQLIssue):
    """oracle线上审核sql文本问题"""

    ENTRIES = (rule.const.RULE_ENTRY_ONLINE_SQL_TEXT,)

    @classmethod
    def simple_analyse(cls, **kwargs) -> Generator["OracleOnlineSQLTextIssue"]:
        task_record_id: int = kwargs["task_record_id"]
        cmdb_id: int = kwargs["cmdb_id"]
        schema_name: str = kwargs["schema_name"]

        with make_session() as session:
            cmdb = session.query(
                OracleCMDB).filter_by(cmdb_id=cmdb_id).first()
            rule_jar: [CMDBRule] = cls.generate_rule_jar()
            entries = cls.inherited_entries()
            with grouped_count_logger(
                    cls.__doc__, item_type_name="rule") as counter:
                for the_rule in rule_jar:
                    docs = []
                    ret = CMDBRuleAdapterSQLOracleOnline(the_rule).run(
                        entries=entries,

                        cmdb=cmdb,
                        single_sql=None,
                        task_record_id=task_record_id,
                        schema_name=schema_name,
                    )
                    for minus_score, output_param in ret:
                        doc = cls()
                        doc.as_issue_of(
                            the_rule,
                            output_data=output_param,
                            minus_score=minus_score
                        )
                        docs.append(doc)
                        yield doc
                    counter(the_rule.name, len(docs))


@OracleOnlineIssue.need_collect()
class OracleOnlineSQLPlanIssue(OracleOnlineSQLExecutionIssue):
    """oracle线上审核sql执行计划问题"""

    ENTRIES = (rule.const.RULE_ENTRY_ONLINE_SQL_PLAN,)


@OracleOnlineIssue.need_collect()
class OracleOnlineSQLStatIssue(OracleOnlineSQLExecutionIssue):
    """oracle线上审核sql执行信息问题"""

    ENTRIES = (rule.const.RULE_ENTRY_ONLINE_SQL_PLAN,)
