# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = ["OracleStatsSchemaRate"]

from typing import NoReturn, Union, List

from mongoengine import FloatField, DictField, BooleanField

import rule.const
from models.sqlalchemy import *
from ..base import *
from .base import *
from oracle_cmdb.issue import OracleOnlineIssue
from ...task.cmdb_task_stats import OracleCMDBTaskStatsEntriesAndRules
from oracle_cmdb.rate import *


@OracleBaseStatistics.need_collect()
class OracleStatsSchemaRate(OracleBaseCurrentTaskSchemaStatistics):
    """schema各个维度的评分"""

    score_average = FloatField(required=True)
    score_lowest = FloatField(required=True)
    entry = DictField(default=dict)
    add_to_rate = BooleanField(default=False)  # 分析时，当前用户是否加入了评分
    rate_info = DictField(default=lambda: {})  # 分析时，当前用户的评分配置信息

    meta = {
        "collection": "oracle_stats_schema_rate",
    }

    ENTRIES_TO_CALC = (rule.const.RULE_ENTRY_ONLINE_OBJECT,
                       rule.const.RULE_ENTRY_ONLINE_SQL_TEXT,
                       rule.const.RULE_ENTRY_ONLINE_SQL_PLAN,
                       rule.const.RULE_ENTRY_ONLINE_SQL_STAT,
                       rule.const.RULE_ENTRY_ONLINE_SQL)

    @classmethod
    def generate(
            cls,
            task_record_id: int,
            cmdb_id: Union[int, None],
            **kwargs) -> NoReturn:
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
            for entry in cls.ENTRIES_TO_CALC:
                issues = OracleOnlineIssue.filter(
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
                doc.entry[entry] = OracleOnlineIssue.calc_score(issues, rule_dicts)
            scores = doc.entry.values()
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
