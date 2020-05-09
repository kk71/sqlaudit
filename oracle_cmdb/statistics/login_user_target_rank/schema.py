# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleStatsSchemaRank"
]

from typing import Generator, Union

from mongoengine import FloatField

from models.sqlalchemy import *
from ..base import OracleBaseStatistics
from .base import OracleStatsMixOfLoginUserAndTargetSchemaRank


# @OracleBaseStatistics.need_collect()
class OracleStatsSchemaRank(OracleStatsMixOfLoginUserAndTargetSchemaRank):
    """用户的纳管库schema排名"""

    score = FloatField()

    meta = {
        "collection": "oracle_stats_schema_rank"
    }

    @classmethod
    def generate(
            cls,
            task_record_id: int,
            cmdb_id: Union[int, None],
            **kwargs) -> Generator["OracleStatsSchemaRank", None, None]:
        pass
