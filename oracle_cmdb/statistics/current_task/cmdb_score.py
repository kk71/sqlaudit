# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleStatsCMDBScore"
]

from typing import Union, Generator

from mongoengine import DictField

import issue
import oracle_cmdb.issue
from ..base import *
from .base import *
from .schema_score import *


@OracleBaseStatistics.need_collect()
class OracleStatsCMDBScore(OracleBaseCurrentTaskStatistics):
    """CMDB加权评分"""

    # 注意，这里涉及的schema并不是登录用户绑定的schema，是纳入评分的schema
    # 因此与登录用户无关
    entry_score = DictField(default=dict)

    meta = {
        "collection": "oracle_stats_cmdb_score",
        "indexes": ["score"]
    }

    REQUIRES = (OracleStatsSchemaScore,)

    ISSUES = (
        issue.OnlineIssue,
        oracle_cmdb.issue.OracleOnlineSQLIssue,
        oracle_cmdb.issue.OracleOnlineObjectIssue
    )
    # 纳管库评分的维度必须是schema的子集
    assert set(ISSUES).issubset(OracleStatsSchemaScore.ISSUES)

    @classmethod
    def generate(
            cls,
            task_record_id: int,
            cmdb_id: Union[int, None],
            **kwargs) -> Generator["OracleStatsCMDBScore", None, None]:
        doc = cls()
        schema_rates = OracleStatsSchemaScore.filter(
            task_record_id=task_record_id,
            add_to_rate=True  # TODO 必须过滤出只计算纳入评分的schema
        )
        for schema_rate in schema_rates:
            # 分数是schema平均分乘以权重之和再除以schema数
            weight = float(dict(schema_rate.rate_info).get("weight", 1))
            for entry in cls.issue_entries():
                if doc.entry_score.get(entry, None) is None:
                    doc.entry_score[entry] = 0
                doc.entry_score[entry] += schema_rate.entry_score[entry] * weight
        schema_count = float(schema_rates.count())
        doc.entry_score = {
            entry: round(score_sum / schema_count, 2)
            for entry, score_sum in doc.entry_score.items()
        }
        cls.post_generated(
            doc=doc,
            task_record_id=task_record_id,
            cmdb_id=cmdb_id
        )
        yield doc

    def cmdb_score(self):
        return self.entry_score.get(
            oracle_cmdb.issue.OracleOnlineIssue.entries[0], None)

