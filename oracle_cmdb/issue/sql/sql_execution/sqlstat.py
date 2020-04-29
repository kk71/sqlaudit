# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleOnlineSQLStatIssue"
]

import rule.const
import cmdb.const
from .sql_execution import OracleOnlineSQLExecutionIssue
from ....issue.base import OracleOnlineIssue
from ....capture import OracleSQLStatToday
from ....task.cmdb_task_stats import *


@OracleOnlineIssue.need_collect()
class OracleOnlineSQLStatIssue(OracleOnlineSQLExecutionIssue):
    """sql执行特征问题"""

    ENTRIES = (rule.const.RULE_ENTRY_ONLINE_SQL_STAT,)

    @classmethod
    def params_to_append_to_rule(cls,
                                 task_record_id: int,
                                 schema_name: str) -> dict:
        # 有stat规则需要snap shot id
        cmdb_task_stats = OracleCMDBTaskStatsSnapIDPairs.objects(
            task_record_id=task_record_id
        ).first()
        return {
            "sql_stat_qs": OracleSQLStatToday.filter(
                task_record_id=task_record_id,
                schema_name=schema_name
            ),
            "snap_ids": cmdb_task_stats.snap_shot_id_pair
        }
