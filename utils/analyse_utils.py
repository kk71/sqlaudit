# Author: kk.Fang(fkfkbill@gmail.com)
"""重构的分析模块"""

__all__ = [
    "calc_statistics"
]

from utils.perf_utils import timing
from plain_db.oracleob import OracleOB
from models.oracle import CMDB, make_session
from models.mongo import *

# 普通采集涉及的模块
CAPTURE_ITEMS = (
    Dashboard,
    DashboardTopStatsDrillDown,
    CMDBOverview
)


@timing()
def calc_statistics(task_record_id):
    """
    计算统计数据
    :param task_record_id:
    :return:
    """
    return


@timing()
def analyse(task_record_id, cmdb_id, schema_name):
    """分析"""
    raise NotImplementedError
