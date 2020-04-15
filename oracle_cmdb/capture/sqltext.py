
# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "SQLText"
]

from typing import NoReturn

from mongoengine import StringField, IntField

from .base import SQLCapturingDoc


@SQLCapturingDoc.need_collect()
class SQLText(SQLCapturingDoc):
    """纳管库sql文本信息"""

    pass
