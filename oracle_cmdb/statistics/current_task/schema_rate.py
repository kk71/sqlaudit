# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = ["OracleStatsSchemaRate"]

from typing import Union, List, Generator

from mongoengine import FloatField, DictField, BooleanField

import oracle_cmdb.issue
from models.sqlalchemy import *
from ..base import *
from .base import *
from ...task.cmdb_task_stats import OracleCMDBTaskStatsEntriesAndRules
from oracle_cmdb.rate import *


@OracleBaseStatistics.need_collect()
class OracleStatsSchemaRate(OracleBaseCurrentTaskSchemaStatistics):
    """schema各个维度的评分"""

    score_average = FloatField(required=True)
    score_lowest = FloatField(required=True)
    entry_score = DictField(default=dict)
    add_to_rate = BooleanField(default=False)  # 分析时，当前用户是否加入了评分
    rate_info = DictField(default=lambda: {})  # 分析时，当前用户的评分配置信息

    meta = {
        "collection": "oracle_stats_schema_rate",
    }

    ISSUES = (
        oracle_cmdb.issue.OracleOnlineObjectIssue,
        oracle_cmdb.issue.OracleOnlineSQLTextIssue,
        oracle_cmdb.issue.OracleOnlineSQLPlanIssue,
        oracle_cmdb.issue.OracleOnlineSQLStatIssue,
        oracle_cmdb.issue.OracleOnlineSQLIssue,
        oracle_cmdb.issue.OracleOnlineObjectIssueIndex,
        oracle_cmdb.issue.OracleOnlineObjectIssueTable,
        oracle_cmdb.issue.OracleOnlineObjectIssueSequence
    )

    @classmethod
    def generate(
            cls,
            task_record_id: int,
            cmdb_id: Union[int, None],
            **kwargs) -> Generator["OracleStatsSchemaRate", None, None]:
        # 因为是针对当前库当前任务的，所以schemas以当前任务的schema为准
        schemas: List[str] = kwargs["schemas"]

        with make_session() as session:
            # 找到需要评分的schema，以及其权重
            rating_schema_q = session.query(OracleRatingSchema).filter(
                OracleRatingSchema.cmdb_id == cmdb_id,
                OracleRatingSchema.schema_name.in_(schemas)
            )
            rating_schemas: dict = {
                i.schema_name: i.to_dict()
                for i in rating_schema_q
            }

        for schema_name in schemas:
            doc = cls()
            for entry in cls.issue_entries():
                print(f"{entry=}")
                issues = oracle_cmdb.issue.OracleOnlineIssue.filter(
                    task_record_id=task_record_id,
                    schema_name=schema_name,
                    entries=entry
                )
                stats_qs = OracleCMDBTaskStatsEntriesAndRules.filter(
                    task_record_id=task_record_id,
                    entries=entry
                )
                rule_dicts = []
                for stats in stats_qs:
                    rule_dicts.extend(stats.rule_info)
                print(f"{rule_dicts=}")
                doc.entry_score[entry] = oracle_cmdb.issue.OracleOnlineIssue.calc_score(
                    issues, rule_dicts)
            scores = doc.entry_score.values()
            doc.score_average = sum(scores) / len(scores)
            doc.score_lowest = min(scores)
            if schema_name in rating_schemas.keys():
                doc.add_to_rate = True
                doc.rate_info = rating_schemas[schema_name]
            cls.post_generated(
                doc=doc,
                task_record_id=task_record_id,
                cmdb_id=cmdb_id,
                schema_name=schema_name)
            yield doc
