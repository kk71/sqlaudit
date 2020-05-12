# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleBaseCurrentTaskStatistics",
    "OracleBaseCurrentTaskSchemaStatistics"
]


from ..current_cmdb import *


class OracleBaseCurrentTaskStatistics(OracleBaseCurrentCMDBStatistics):
    """当前任务库的统计"""

    meta = {
        "abstract": True
    }


class OracleBaseCurrentTaskSchemaStatistics(
        OracleBaseCurrentTaskStatistics,
        OracleBaseCurrentCMDBSchemaStatistics):
    """当前任务库的schema的统计"""

    meta = {
        "abstract": True
    }
