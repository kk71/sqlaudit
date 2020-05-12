# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = ["OracleStatsCMDBScore"]

from typing import Union, Generator

from mongoengine import FloatField

import rule.const
from ..base import *
from .base import *
from .schema_score import *


@OracleBaseStatistics.need_collect()
class OracleStatsCMDBScore(OracleBaseCurrentTaskStatistics):
    """CMDB加权评分"""

    score = FloatField(default=0)
    score_sql = FloatField(default=0)
    score_obj = FloatField(default=0)

    meta = {
        "collection": "oracle_stats_cmdb_rate",
        "indexes": ["score"]
    }

    REQUIRES = (OracleStatsSchemaScore,)

    @classmethod
    def generate(
            cls,
            task_record_id: int,
            cmdb_id: Union[int, None],
            **kwargs) -> Generator["OracleStatsCMDBScore", None, None]:
        doc = cls()
        schema_rates = OracleStatsSchemaScore.objects(
            task_record_id=task_record_id,
            add_to_rate=True  # 只计算纳入评分的schema
        )
        schema_num = 0
        for schema_rate in schema_rates:
            # 三个分数都是schema平均分乘以权重之和再除以schema数
            weight = float(dict(schema_rate.rate_info).get("weight", 1))
            doc.score += schema_rate.get_schema_score() * weight
            doc.score_sql += schema_rate.entry_score.get(
                rule.const.RULE_ENTRY_ONLINE_SQL, 0) * weight
            doc.score_obj += schema_rate.entry_score.get(
                rule.const.RULE_ENTRY_ONLINE_OBJECT, 0) * weight
            schema_num += 1
        if schema_num:
            schema_num = float(schema_num)
            doc.score = round(doc.score / schema_num, 2)
            doc.score_sql = round(doc.score_sql / schema_num, 2)
            doc.score_obj = round(doc.score_obj / schema_num, 2)
        cls.post_generated(
            doc=doc,
            task_record_id=task_record_id,
            cmdb_id=cmdb_id)
        yield doc
