# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleOnlineSQLExecutionIssue"
]

import abc
from typing import Generator

from mongoengine import IntField, EmbeddedDocumentField

from models.sqlalchemy import *
from oracle_cmdb.cmdb import *
from oracle_cmdb.issue.sql import OracleOnlineSQLIssue, OracleOnlineIssueOutputParamsSQL
from rule.cmdb_rule import CMDBRule
from rule.adapters import CMDBRuleAdapterSQL


class OracleOnlineIssueOutputParamsSQLExecution(
        OracleOnlineIssueOutputParamsSQL):
    """针对SQL执行相关的输出字段"""

    plan_hash_value = IntField(required=True, default=None)


class OracleOnlineSQLExecutionIssue(OracleOnlineSQLIssue):
    """sql执行相关问题"""

    output_params = EmbeddedDocumentField(
        OracleOnlineIssueOutputParamsSQLExecution,
        default=OracleOnlineIssueOutputParamsSQLExecution)

    meta = {
        "allow_inheritance": True
    }

    @classmethod
    @abc.abstractmethod
    def params_to_append_to_rule(cls,
                                 task_record_id: int,
                                 schema_name: str) -> dict:
        pass

    @classmethod
    def simple_analyse(cls, **kwargs) -> Generator[
            "OracleOnlineSQLExecutionIssue", None, None]:
        task_record_id: int = kwargs["task_record_id"]
        cmdb_id: int = kwargs["cmdb_id"]
        schema_name: str = kwargs["schema_name"]

        rule_jar: [CMDBRule] = cls.generate_rule_jar(
            cmdb_id,
            task_record_id=task_record_id,
            schema_name=schema_name
        )
        with make_session() as session:
            the_cmdb = session.query(
                OracleCMDB).filter_by(cmdb_id=cmdb_id).first()
            cmdb_connector = the_cmdb.build_connector()
            for the_rule in rule_jar:
                ret = CMDBRuleAdapterSQL(the_rule).run(
                    entries=rule_jar.entries,

                    cmdb=the_cmdb,
                    task_record_id=task_record_id,
                    schema_name=schema_name,
                    cmdb_connector=cmdb_connector,
                    **cls.params_to_append_to_rule(task_record_id, schema_name)
                )
                docs = cls.pack_rule_ret_to_doc(the_rule, ret)
                cls.post_analysed(
                    docs=docs,
                    task_record_id=task_record_id,
                    schema_name=schema_name,
                    cmdb_id=cmdb_id
                )
                yield from docs
