# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleStatsEntryScore"
]

from typing import Union, List, Generator

from mongoengine import DictField

from models.sqlalchemy import *
import oracle_cmdb.issue
from .base import *
from ..base import *
from ...task.cmdb_task_stats import OracleCMDBTaskStatsEntriesAndRules


@OracleBaseStatistics.need_collect()
class OracleStatsEntryScore(OracleStatsMixOfLoginUserAndCurrentTask):
    """维度分数"""

    entry_score = DictField(default=dict)

    meta = {
        "collection": "oracle_stats_entry_score",
        "indexes": [
            "entry"
        ]
    }

    ISSUES = (
        oracle_cmdb.issue.OracleOnlineObjectIssue,
        oracle_cmdb.issue.OracleOnlineSQLTextIssue,
        oracle_cmdb.issue.OracleOnlineSQLPlanIssue,
        oracle_cmdb.issue.OracleOnlineSQLStatIssue
    )

    @classmethod
    def generate(
            cls,
            task_record_id: int,
            cmdb_id: Union[int, None],
            **kwargs) -> Generator["OracleStatsEntryScore", None, None]:
        with make_session() as session:
            for the_user in cls.users(session, cmdb_id=cmdb_id):
                schemas: List[str] = list(cls.schemas(
                    session,
                    cmdb_id=cmdb_id,
                    login_user=the_user.login_user
                ))
                doc = cls()
                for entry in cls.issue_entries():
                    issues = oracle_cmdb.issue.OracleOnlineIssue.filter(
                        task_record_id=task_record_id,
                        schema_name__in=schemas,
                        entries=entry
                    )
                    stats_qs = OracleCMDBTaskStatsEntriesAndRules.filter(
                        task_record_id=task_record_id,
                        schema_name__in=schemas,
                        rule_info__entries=entry
                    )
                    rule_dicts = []
                    for stats in stats_qs:
                        rule_dicts.extend(stats.rule_info)
                    doc.entry_score[entry] = oracle_cmdb.issue.\
                        OracleOnlineSQLIssue.calc_score(issues, rule_dicts)
                cls.post_generated(
                    doc=doc,
                    cmdb_id=cmdb_id,
                    task_record_id=task_record_id,
                    target_login_user=the_user.login_user
                )
                yield doc

