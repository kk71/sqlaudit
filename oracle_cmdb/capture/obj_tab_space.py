# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "ObjTabSpace"
]

from typing import NoReturn

from mongoengine import StringField, FloatField

from .base import *


class ObjTabSpace(ObjectCapturingDoc):
    """表空间信息"""

    tablespace_name = StringField()
    total = FloatField(help_text="bytes")
    free = FloatField(help_text="bytes")
    used = FloatField(help_text="bytes")
    usage_ratio = FloatField()

    meta = {
        "collection": "obj_tab_space",
        "indexes": ["tablespace_name"]
    }

    @classmethod
    def simple_capture(cls, **kwargs) -> str:
        return f"""
        SELECT a.tablespace_name as tablespace_name,
       total,
       free,
       (total - free) as used,
       Round((total - free) / total, 4) as usage_ratio
  FROM (SELECT tablespace_name, Sum(bytes) free
          FROM DBA_FREE_SPACE
         GROUP BY tablespace_name) a,
       (SELECT tablespace_name, Sum(bytes) total
          FROM DBA_DATA_FILES
         GROUP BY tablespace_name) b
 WHERE a.tablespace_name = b.tablespace_name
"""

    @classmethod
    def post_captured(cls, **kwargs) -> NoReturn:
        ObjectCapturingDoc.post_captured(**kwargs)
        docs: ["ObjTabSpace"] = kwargs["docs"]
        for d in docs:
            d.total = float(d.total)
            d.free = float(d.free)
            d.used = float(d.used)
