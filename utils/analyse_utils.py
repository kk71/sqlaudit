# Author: kk.Fang(fkfkbill@gmail.com)
"""重构的分析模块"""

__all__ = [
    "calc_statistics"
]

from utils.perf_utils import timing
from models.mongo import *

# 统计数据model
# 注意：如果统计数据有先后依赖，需要在这里体现。
STATS_MODELS = (
    StatsNumDrillDown,
    StatsCMDBPhySize,
    StatsLoginUser,
)


@timing()
def calc_statistics(*args, **kwargs):
    """
    计算统计数据
    :return:
    """
    for m in STATS_MODELS:
        print(f"* gonna make statistics data for {m.__doc__} ...")
        an_iterator = m.generate(*args, **kwargs)
        if an_iterator is None:
            print("Returned None, should be an iterator. Skipped.")
            continue
        docs = list(an_iterator)
        if not docs:
            print("no statistics object to be saved.")
            continue
        m.objects.insert(docs)


@timing()
def analyse(task_record_id, cmdb_id, schema_name):
    """分析"""
    raise NotImplementedError
