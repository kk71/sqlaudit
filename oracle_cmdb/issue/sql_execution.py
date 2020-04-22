# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleOnlineSQLExecutionIssue"
]

from typing import NoReturn, Generator

from mongoengine import IntField, EmbeddedDocumentField

from models.sqlalchemy import *
from ..cmdb import *
from .sql import OracleOnlineSQLIssue, OracleOnlineIssueOutputParamsSQL
from rule.cmdb_rule import CMDBRule
from rule.adapters import CMDBRuleAdapterSQL


class OracleOnlineIssueOutputParamsSQLExecution(
        OracleOnlineIssueOutputParamsSQL):
    """针对SQL执行相关的输出字段"""

    plan_hash_value = IntField(required=True, default=None)


class OracleOnlineSQLExecutionIssue(OracleOnlineSQLIssue):
    """sql执行相关问题"""

    output_params = EmbeddedDocumentField(
        OracleOnlineIssueOutputParamsSQLExecution)

    meta = {
        "allow_inheritance": True
    }

    @classmethod
    def simple_analyse(cls, **kwargs) -> Generator[
            "OracleOnlineSQLExecutionIssue", None, None]:
        task_record_id: int = kwargs["task_record_id"]
        cmdb_id: int = kwargs["cmdb_id"]
        schema_name: str = kwargs["schema_name"]

        rule_jar: [CMDBRule] = cls.generate_rule_jar()
        with make_session() as session:
            the_cmdb = session.query(
                OracleCMDB).filter_by(cmdb_id=cmdb_id).first()
            for the_rule in rule_jar:
                ret = CMDBRuleAdapterSQL(the_rule).run(
                    entries=rule_jar.entries,

                    cmdb=the_cmdb,
                    task_record_id=task_record_id,
                    schema_name=schema_name
                )
                docs = cls.pack_rule_ret_to_doc(the_rule, ret)
                cls.post_analysed(
                    docs=docs,
                    task_record_id=task_record_id,
                    schema_name=schema_name,
                    cmdb_id=cmdb_id
                )
                yield from docs

