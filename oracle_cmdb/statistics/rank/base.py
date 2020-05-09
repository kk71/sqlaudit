# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleStatsRank"
]

from typing import List

from mongoengine import IntField

from ..base import *


class OracleStatsRank(OracleBaseStatistics):
    """排名"""

    rank = IntField(default=0)

    meta = {
        "abstract": True,
        "indexes": [
            "rank"
        ]
    }

    @classmethod
    def post_generated(cls, **kwargs):
        doc: List["OracleStatsRank"] = kwargs["doc"]
        rank: int = kwargs["rank"]

        doc.rank = rank

    @classmethod
    def filter(cls, *args, **kwargs):
        # rank是排名，递增的顺序就是默认想要展示的顺序
        return super().filter(*args, **kwargs).order_by("rank")
