# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleOnlineSQLStatIssue"
]

import rule.const
from oracle_cmdb.issue.base import OracleOnlineIssue
from oracle_cmdb.issue.sql.sql_execution import OracleOnlineSQLExecutionIssue
from oracle_cmdb.capture import OracleSQLStat
from oracle_cmdb import const


@OracleOnlineIssue.need_collect()
class OracleOnlineSQLStatIssue(OracleOnlineSQLExecutionIssue):
    """sql执行特征问题"""

    ENTRIES = (rule.const.RULE_ENTRY_ONLINE_SQL_STAT,)

    @classmethod
    def params_to_append_to_rule(cls,
                                 task_record_id: int,
                                 schema_name: str) -> dict:
        return {
            "sql_stat_qs": OracleSQLStat.objects(
                two_days_capture=const.SQL_TWO_DAYS_CAPTURE_TODAY,
                task_record_id=task_record_id,
                schema_name=schema_name
            )
        }
