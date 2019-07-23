# Author: kk.Fang(fkfkbill@gmail.com)

"""
统计信息，用于接口快速获取数据

编码注意：所有涉及sqlalchemy的import，必须在函数内！
"""

from mongoengine import IntField, StringField, ObjectIdField, DateTimeField, \
    DynamicField, FloatField, LongField

from .utils import BaseStatisticsDoc


class StatsDashboard(BaseStatisticsDoc):
    """仪表盘统计数据"""



    meta = {
        "collection": "stats_dashboard"
    }

    @classmethod
    def generate(cls, task_record_id: int):
        return


class StatsDashboardDrillDown(BaseStatisticsDoc):
    """仪表盘四个数据的下钻"""

    meta = {
        "collection": "stats_dashboard_drill_down"
    }


class StatsCMDBOverview(BaseStatisticsDoc):
    """纳管库概览页统计数据"""

    meta = {
        "collection": "stats_cmdb_overview"
    }


class StatsCMDBOverviewTabSpace(BaseStatisticsDoc):
    """纳管数据库概览页表空间数据"""

    meta = {
        "collection": "stats_cmdb_overview_tab_space"
    }
