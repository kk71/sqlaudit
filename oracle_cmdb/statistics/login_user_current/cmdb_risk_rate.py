# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleStatsCMDBRiskRate"
]

from mongoengine import IntField, ListField

from ..base import *
from .base import *


# @OracleBaseStatistics.need_collect()
class OracleStatsCMDBRiskRate(OracleStatsMixOfLoginUserAndCurrentCMDB):
    """登录用户与当前库的风险率统计"""

    pass
