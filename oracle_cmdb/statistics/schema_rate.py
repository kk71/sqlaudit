# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = ["StatsSchemaRate"]

from collections import defaultdict

from mongoengine import FloatField, DictField, BooleanField

from .base import *
from ..issue import OracleOnlineIssue


class StatsSchemaRate(OracleBaseSchemaStatistics):
    """纳管schema的评分"""

    score_average = FloatField(required=True)
    score_lowest = FloatField(required=True)
    entry = DictField(default=dict)
    # score_rule_type = DictField(default=lambda: {})
    # drill_down_type = DictField(default=lambda: {})
    add_to_rate = BooleanField(default=False)  # 分析时，当前用户是否加入了评分？
    rate_info = DictField(default=lambda: {})  # 分析时，当前用户的评分配置信息

    meta = {
        "collection": "stats_schema_rate",
    }

    @classmethod
    def process(cls, collected=None, **kwargs):
        pass
