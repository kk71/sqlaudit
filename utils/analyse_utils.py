# Author: kk.Fang(fkfkbill@gmail.com)
"""重构的分析模块"""

__all__ = [
    "calc_statistics"
]

from utils import const
from utils.perf_utils import timing
# from models. import *
#
# 统计数据model
# 注意：如果统计数据有先后依赖，需要在这里体现。
# STATS_MODELS = (
#     StatsSchemaRate,
#     StatsCMDBRate,
#     StatsCMDBSQLText,
#     StatsRiskSqlRule,
#     StatsRiskObjectsRule,
#     StatsNumDrillDown,
#     StatsCMDBPhySize,
#     StatsCMDBLoginUser,
#     StatsCMDBSQLPlan,
#     StatsLoginUser
# )


def check_for_requirement():
    """
    检查统计模块的依赖关系和运行顺序是否正常
    :return:
    """
    print("statistics models requirement checking ...")
    processed_models = []
    for m in STATS_MODELS:
        required_models_but_not_ready = [
            required_model
            for required_model in m.requires
            if required_model not in processed_models
        ]
        if required_models_but_not_ready:
            print(f"Failing: {m} requires {required_models_but_not_ready} to run first!")
            raise const.RequiredModelNotRunException
        processed_models.append(m)


@timing()
def calc_statistics(*args, **kwargs):
    """
    计算统计数据
    :return:
    """
    check_for_requirement()
    for m in STATS_MODELS:
        print(f"* Making statistics data for {m.__doc__} ...")
        an_iterator = m.generate(*args, **kwargs)
        if an_iterator is None:
            print("Returned None, should be an iterator. Skipped.")
            continue
        docs = list(an_iterator)
        if not docs:
            print("No statistics object to be saved.")
            continue
        m.objects.insert(docs)


@timing()
def analyse(task_record_id, cmdb_id, schema_name):
    """分析"""
    raise NotImplementedError
