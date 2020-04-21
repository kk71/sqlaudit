# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleOnlineSQLStatIssue"
]

from typing import Generator

import rule.const
from ..cmdb import OracleCMDB
from .base import OracleOnlineIssue
from .sql_execution import OracleOnlineSQLExecutionIssue
from rule import CMDBRule
from rule.adapters import CMDBRuleAdapterSQL


@OracleOnlineIssue.need_collect()
class OracleOnlineSQLStatIssue(OracleOnlineSQLExecutionIssue):
    """sql执行信息问题"""

    ENTRIES = (rule.const.RULE_ENTRY_ONLINE_SQL_PLAN,)

    @classmethod
    def _single_rule_analyse(
            cls,
            the_rule: CMDBRule,
            entries: [str],
            the_cmdb: OracleCMDB,
            task_record_id: int,
            schema_name: str,
            **kwargs) -> Generator["OracleOnlineSQLStatIssue", None, None]:
        sql_ids: [str] = kwargs["sql_ids"]

        for sql_id in sql_ids:
            ret = CMDBRuleAdapterSQL(the_rule).run(
                entries=entries,

                cmdb=the_cmdb,
                task_record_id=task_record_id,
                schema_name=schema_name,
                sql_stat_qs=cls.get_sql_stat_qs(task_record_id, sql_id)
            )
            yield from cls.pack_rule_ret_to_doc(the_rule, ret)