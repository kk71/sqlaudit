# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = ["OracleStatsCMDBPhySize"]

from typing import Union, Generator

from mongoengine import FloatField

from ..base import *
from .base import *
from ...capture import OracleObjTabSpace


@OracleBaseStatistics.need_collect()
class OracleStatsCMDBPhySize(OracleBaseCurrentTaskCMDBStatistics):
    """CMDB容量信息"""

    total = FloatField(help_text="bytes")
    free = FloatField(help_text="bytes")
    used = FloatField(help_text="bytes")
    usage_ratio = FloatField()

    meta = {
        "collection": "oracle_stats_cmdb_phy_size"
    }

    @classmethod
    def generate(
            cls,
            task_record_id: int,
            cmdb_id: Union[int, None],
            **kwargs) -> Generator["OracleStatsCMDBPhySize", None, None]:
        doc = cls(
            total=0,
            free=0,
            used=0,
            usage_ratio=0
        )
        for ts in OracleObjTabSpace.objects(
                task_record_id=task_record_id, cmdb_id=cmdb_id).all():
            doc.total += ts.total
            doc.free += ts.free
            doc.used += ts.used
        doc.usage_ratio = doc.used / doc.total
        cls.post_generated(
            doc=doc,
            task_record_id=task_record_id,
            cmdb_id=cmdb_id)
        yield doc
