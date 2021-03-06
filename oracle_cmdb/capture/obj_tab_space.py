# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleObjTabSpace"
]

from typing import NoReturn, Tuple, Optional

from mongoengine import StringField, FloatField

from .. import const
from .base import OracleObjectCapturingDoc


@OracleObjectCapturingDoc.need_collect()
class OracleObjTabSpace(OracleObjectCapturingDoc):
    """表空间信息"""

    tablespace_name = StringField(null=True)
    total = FloatField(help_text="bytes", null=True)
    free = FloatField(help_text="bytes", null=True)
    used = FloatField(help_text="bytes", null=True)
    usage_ratio = FloatField()

    meta = {
        "collection": "oracle_obj_tab_space",
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
        OracleObjectCapturingDoc.post_captured(**kwargs)
        docs: ["OracleObjTabSpace"] = kwargs["docs"]
        for d in docs:
            d.total = float(d.total)
            d.free = float(d.free)
            d.used = float(d.used)

    def get_object_unique_name(self) -> Tuple[Optional[str], str, str]:
        return None, const.ORACLE_OBJECT_TYPE_TABLESPACE, self.tablespace_name
