# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = ["OracleStatsCMDBRate"]

from typing import NoReturn, Union

from mongoengine import FloatField

import rule.const
from ..base import *
from .base import *
from .schema_rate import *


@OracleBaseStatistics.need_collect()
class OracleStatsCMDBRate(OracleBaseCurrentTaskCMDBStatistics):
    """CMDB加权评分"""

    score = FloatField(default=0)
    score_sql = FloatField(default=0)
    score_obj = FloatField(default=0)

    meta = {
        "collection": "oracle_stats_cmdb_rate",
        "indexes": ["score"]
    }

    REQUIRES = (OracleStatsSchemaRate,)

    @classmethod
    def generate(
            cls,
            task_record_id: int,
            cmdb_id: Union[int, None],
            **kwargs) -> NoReturn:
        doc = cls()
        schema_rates = OracleStatsSchemaRate.objects(
            task_record_id=task_record_id,
            add_to_rate=True  # 只计算纳入评分的schema
        )
        schema_num = 0
        for schema_rate in schema_rates:
            # 三个分数都是schema平均分乘以权重之和再除以schema数
            weight = float(dict(schema_rate.rate_info).get("weight", 1))
            doc.score += schema_rate.score_average * weight
            doc.score_sql += schema_rate.entry.get(
                rule.const.RULE_ENTRY_ONLINE_SQL, 0) * weight
            doc.score_obj += schema_rate.entry.get(
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
