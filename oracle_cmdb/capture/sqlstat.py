# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "SQLStat"
]

from typing import NoReturn

from mongoengine import StringField, IntField

from .base import TwoDaysSQLCapturingDoc


@TwoDaysSQLCapturingDoc.need_collect()
class SQLStat(TwoDaysSQLCapturingDoc):
    """纳管库sql执行信息"""

    pass
