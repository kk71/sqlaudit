# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleOnlineSQLPlanIssue"
]

import rule.const
from oracle_cmdb.issue.base import OracleOnlineIssue
from oracle_cmdb.issue.sql.sql_execution import OracleOnlineSQLExecutionIssue
from oracle_cmdb.capture import OracleSQLPlanToday


@OracleOnlineIssue.need_collect()
class OracleOnlineSQLPlanIssue(OracleOnlineSQLExecutionIssue):
    """sql执行计划问题"""

    ENTRIES = (rule.const.RULE_ENTRY_ONLINE_SQL_PLAN,)

    @classmethod
    def params_to_append_to_rule(cls,
                                 task_record_id: int,
                                 schema_name: str) -> dict:
        return {
            "sql_plan_qs": OracleSQLPlanToday.filter(
                task_record_id=task_record_id,
                schema_name=schema_name
            )
        }
