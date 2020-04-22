# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleOnlineSQLPlanIssue"
]

import rule.const
from .base import OracleOnlineIssue
from .sql_execution import OracleOnlineSQLExecutionIssue


@OracleOnlineIssue.need_collect()
class OracleOnlineSQLPlanIssue(OracleOnlineSQLExecutionIssue):
    """sql执行计划问题"""

    ENTRIES = (rule.const.RULE_ENTRY_ONLINE_SQL_PLAN,)

