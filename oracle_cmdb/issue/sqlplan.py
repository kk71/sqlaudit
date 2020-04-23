# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleOnlineSQLPlanIssue"
]

import rule.const
from .base import OracleOnlineIssue
from .sql_execution import OracleOnlineSQLExecutionIssue
from ..capture import SQLPlan
from .. import const


@OracleOnlineIssue.need_collect()
class OracleOnlineSQLPlanIssue(OracleOnlineSQLExecutionIssue):
    """sql执行计划问题"""

    ENTRIES = (rule.const.RULE_ENTRY_ONLINE_SQL_PLAN,)

    @classmethod
    def params_to_append_to_rule(cls,
                                 task_record_id: int,
                                 schema_name: str) -> dict:
        return {
            "sql_plan_qs": SQLPlan.objects(
                two_days_capture=const.SQL_TWO_DAYS_CAPTURE_TODAY,
                task_record_id=task_record_id,
                schema_name=schema_name
            )
        }
