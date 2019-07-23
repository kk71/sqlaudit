# Author: kk.Fang(fkfkbill@gmail.com)
"""重构的分析模块"""

__all__ = [
    "calc_statistics"
]

from utils.perf_utils import timing
from plain_db.oracleob import OracleOB
from models.oracle import CMDB, make_session
from models.mongo import *

# 统计数据model
STATS_MODELS = (
    StatsDashboard,
    StatsDashboardDrillDown,
    StatsCMDBOverview,
    StatsCMDBOverviewTabSpace
)


@timing()
def calc_statistics(task_record_id):
    """
    计算统计数据
    :param task_record_id:
    :return:
    """
    for m in STATS_MODELS:
        print(f"* gonna make statistics data for {m.__doc__} ...")
        m.generate(task_record_id)


@timing()
def analyse(task_record_id, cmdb_id, schema_name):
    """分析"""
    raise NotImplementedError
