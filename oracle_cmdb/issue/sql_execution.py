# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleOnlineSQLExecutionIssue"
]

from typing import NoReturn, Generator

from mongoengine import IntField

from models.sqlalchemy import *
from models.mongoengine import *
from ..capture import SQLPlan, SQLStat
from ..cmdb import OracleCMDB
from .sql import OracleOnlineSQLIssue
from rule import CMDBRule
from utils.log_utils import *


class OracleOnlineSQLExecutionIssue(OracleOnlineSQLIssue):
    """sql运行问题"""

    plan_hash_value = IntField(required=True, default=None)

    meta = {
        "allow_inheritance": True,
        "indexes": [
            "plan_hash_value"
        ]
    }

    @classmethod
    def post_analysed(cls, **kwargs) -> NoReturn:
        docs: ["OracleOnlineSQLExecutionIssue"] = kwargs["docs"]
        plan_hash_value: int = kwargs["plan_hash_value"]

        super().post_analysed(**kwargs)
        for doc in docs:
            doc.plan_hash_value = plan_hash_value

    @classmethod
    def get_sql_plan_qs(cls,
                        task_record_id: int,
                        sql_id: str = None) -> Generator[mongoengine_qs, None, None]:
        plan_hash_values = SQLPlan.objects(
            task_record_id=task_record_id, sql_id=sql_id).distinct(
            "plan_hash_value")
        for phv in plan_hash_values:
            yield SQLPlan.objects(
                task_record_id=task_record_id,
                sql_id=sql_id,
                plan_hash_value=phv
            )

    @classmethod
    def get_sql_stat_qs(cls,
                        task_record_id: int,
                        sql_id: str = None) -> Generator[mongoengine_qs, None, None]:
        plan_hash_values = SQLStat.objects(
            task_record_id=task_record_id, sql_id=sql_id).distinct(
            "plan_hash_value")
        for phv in plan_hash_values:
            yield SQLStat.objects(
                task_record_id=task_record_id,
                sql_id=sql_id,
                plan_hash_value=phv
            )

    @classmethod
    def simple_analyse(cls, **kwargs) -> Generator["OracleOnlineSQLExecutionIssue", None, None]:
        task_record_id: int = kwargs["task_record_id"]
        cmdb_id: int = kwargs["cmdb_id"]
        schema_name: str = kwargs["schema_name"]

        rule_jar: [CMDBRule] = cls.generate_rule_jar()
        entries = cls.inherited_entries()
        sql_ids: [str] = cls.get_sql_qs(
            task_record_id, schema_name).distinct("sql_id")
        with make_session() as session:
            the_cmdb = session.query(
                OracleCMDB).filter_by(cmdb_id=cmdb_id).first()
            with grouped_count_logger(
                    cls.__doc__, item_type_name="rule") as counter:
                for the_rule in rule_jar:
                    docs = list(cls._single_rule_analyse(
                        the_rule=the_rule,
                        entries=entries,
                        the_cmdb=the_cmdb,
                        task_record_id=task_record_id,
                        schema_name=schema_name,
                        sql_ids=sql_ids
                    ))
                    counter(the_rule.name, len(docs))
                    yield from docs