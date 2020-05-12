# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleStatsCMDBSQLExecutionCostRank"
]

from typing import Generator, Union

from mongoengine import IntField, ListField

from ..base import *
from .base import *


# @OracleBaseStatistics.need_collect()
class OracleStatsCMDBSQLExecutionCostRank(
        OracleStatsMixOfLoginUserAndCurrentCMDB):
    """登录用户与当前库的SQL执行效率排名统计"""

    date_period = IntField(help_text="时间区间", choices=DATE_PERIOD)
    active = ListField(default=list)
    at_risk = ListField(default=list)

    @classmethod
    def generate(
            cls,
            task_record_id: int,
            cmdb_id: Union[int, None],
            **kwargs) -> Generator[
                "OracleStatsCMDBSQLExecutionCostRank", None, None]:
        pass
