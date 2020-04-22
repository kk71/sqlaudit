# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleOnlineSQLStatIssue"
]

import rule.const
from .base import OracleOnlineIssue
from .sql_execution import OracleOnlineSQLExecutionIssue


@OracleOnlineIssue.need_collect()
class OracleOnlineSQLStatIssue(OracleOnlineSQLExecutionIssue):
    """sql执行特征问题"""

    ENTRIES = (rule.const.RULE_ENTRY_ONLINE_SQL_STAT,)

