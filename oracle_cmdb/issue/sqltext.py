# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleOnlineSQLTextIssue"
]

from typing import Generator

import rule.const
from models.sqlalchemy import *
from ..cmdb import OracleCMDB
from .base import OracleOnlineIssue
from .sql import OracleOnlineSQLIssue
from ..single_sql import SingleSQLForOnline
from rule.cmdb_rule import CMDBRule
from rule.adapters import CMDBRuleAdapterSQL
from utils.log_utils import *


@OracleOnlineIssue.need_collect()
class OracleOnlineSQLTextIssue(OracleOnlineSQLIssue):
    """sql文本问题"""

    ENTRIES = (rule.const.RULE_ENTRY_ONLINE_SQL_TEXT,)

    @classmethod
    def _single_rule_analyse(
            cls,
            the_rule: CMDBRule,
            entries: [str],
            the_cmdb: OracleCMDB,
            task_record_id: int,
            schema_name: str,
            **kwargs) -> Generator["OracleOnlineSQLTextIssue", None, None]:
        single_sqls: [SingleSQLForOnline] = kwargs["single_sqls"]

        for single_sql in single_sqls:
            ret = CMDBRuleAdapterSQL(the_rule).run(
                entries=entries,

                cmdb=the_cmdb,
                single_sql=single_sql,
                task_record_id=task_record_id,
                schema_name=schema_name,
            )
            yield from cls.pack_rule_ret_to_doc(the_rule, ret)

    @classmethod
    def simple_analyse(cls, **kwargs) -> Generator["OracleOnlineSQLTextIssue", None, None]:
        task_record_id: int = kwargs["task_record_id"]
        cmdb_id: int = kwargs["cmdb_id"]
        schema_name: str = kwargs["schema_name"]

        rule_jar: [CMDBRule] = cls.generate_rule_jar()
        entries = cls.inherited_entries()
        single_sqls = [
            SingleSQLForOnline.gen_from_sql_text(a_sql_text)
            for a_sql_text in cls.get_sql_qs(task_record_id, schema_name)
        ]
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
                        single_sqls=single_sqls
                    ))
                    counter(the_rule.name, len(docs))
                    yield from docs