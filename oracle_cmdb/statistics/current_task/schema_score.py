# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleStatsSchemaScore"
]

from typing import Union, List, Generator

from mongoengine import DictField, BooleanField

import issue
from ...issue import *
from models.sqlalchemy import *
from ..base import *
from .base import *
from oracle_cmdb.rate import *


@OracleBaseStatistics.need_collect()
class OracleStatsSchemaScore(OracleBaseCurrentTaskSchemaStatistics):
    """schema各个维度的评分"""

    entry_score = DictField(default=dict)
    # TODO 请注意是否加入评分这个选项。仅仅在计算纳管库分数的时候！才过滤该字段！
    # TODO 其他地方一律用登录用户绑定纳管库的schema来展示评分，包括统计的分数
    add_to_rate = BooleanField(default=False)  # 分析时，当前用户是否加入了评分
    rate_info = DictField(default=lambda: {})  # 分析时，当前用户的评分配置信息

    meta = {
        "collection": "oracle_stats_schema_score",
    }

    ISSUES = (
        issue.OnlineIssue,
        OracleOnlineObjectIssue,
        OracleOnlineSQLTextIssue,
        OracleOnlineSQLPlanIssue,
        OracleOnlineSQLStatIssue,
        OracleOnlineSQLIssue,
        OracleOnlineObjectIssueIndex,
        OracleOnlineObjectIssueTable,
        OracleOnlineObjectIssueSequence
    )

    @classmethod
    def generate(
            cls,
            task_record_id: int,
            cmdb_id: Union[int, None],
            **kwargs) -> Generator["OracleStatsSchemaScore", None, None]:

        from oracle_cmdb.tasks.capture.cmdb_task_stats import \
            OracleCMDBTaskStatsEntriesAndRules

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
                issues = oracle_cmdb.issue.OracleOnlineIssue.filter(
                    task_record_id=task_record_id,
                    schema_name=schema_name,
                    entries=entry
                )
                stats_qs = OracleCMDBTaskStatsEntriesAndRules.filter(
                    task_record_id=task_record_id,
                    rule_info__entries=entry
                )
                rule_dicts = []
                for stats in stats_qs:
                    rule_dicts.extend(stats.rule_info)
                doc.entry_score[entry] = oracle_cmdb.issue.OracleOnlineIssue.calc_score(
                    issues, rule_dicts)
            if schema_name in rating_schemas.keys():
                doc.add_to_rate = True
                doc.rate_info = rating_schemas[schema_name]
            cls.post_generated(
                doc=doc,
                task_record_id=task_record_id,
                cmdb_id=cmdb_id,
                schema_name=schema_name
            )
            yield doc

    def schema_score(self):
        return self.entry_score.get(
            issue.OnlineIssue.ENTRIES[0], None)

