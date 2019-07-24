# Author: kk.Fang(fkfkbill@gmail.com)

"""
统计信息，用于接口快速获取数据

编码注意：所有涉及sqlalchemy的import，必须在函数内！
"""

from collections import defaultdict

from mongoengine import IntField, StringField, DateTimeField, \
    DynamicField, FloatField, LongField

from utils.const import *
from .utils import BaseStatisticsDoc


class StatsDashboard(BaseStatisticsDoc):
    """仪表盘统计数据"""

    login_user = StringField()
    sql_num = LongField()
    tab_num = IntField()
    index_num = LongField()
    seq_num = IntField()

    meta = {
        "collection": "stats_dashboard"
    }

    @classmethod
    def generate(cls, task_record_id: int) -> list:
        return []


class StatsDashboardDrillDown(BaseStatisticsDoc):
    """仪表盘四个数据的下钻"""

    drill_down_type = StringField(choices=ALL_DASHBOARD_STATS_NUM_TYPE)
    cmdb_id = IntField()
    connect_name = StringField()
    schema_name = StringField()
    num = LongField()

    meta = {
        "collection": "stats_dashboard_drill_down"
    }

    @classmethod
    def generate(cls, task_record_id: int) -> list:
        from utils.score_utils import get_latest_task_record_id, get_result_object_by_type
        from models.oracle import make_session, DataPrivilege, CMDB, QueryEntity
        from models.mongo import SQLText, ObjSeqInfo, ObjTabInfo, ObjIndColInfo

        with make_session() as session:
            # the type: cmdb_id: schema_name: num
            num_dict = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: 0)))
            qe = QueryEntity([
                CMDB.cmdb_id,
                CMDB.connect_name,
                DataPrivilege.schema_name
            ])
            rst = session.query(*qe).join(CMDB, DataPrivilege.cmdb_id == CMDB.cmdb_id)
            cmdb_ids = [i["cmdb_id"] for i in qe.to_dict(rst)]
            cmdb_id_cmdb_info_dict = {i["cmdb_id"]: i for i in qe.to_dict(rst)}
            latest_task_record_id_dict = get_latest_task_record_id(session, cmdb_ids)
            for cmdb_id, connect_name, schema_name in set(rst):  # 必须去重
                latest_task_record_id = latest_task_record_id_dict[cmdb_id]
                for t in ALL_DASHBOARD_STATS_NUM_TYPE:
                    # 因为results不同的类型结构不一样，需要分别处理
                    if t == DASHBOARD_STATS_NUM_SQL:
                        num_dict[t][cmdb_id][schema_name] += len(
                            SQLText.filter_by_exec_hist_id(latest_task_record_id).
                            filter(cmdb_id=cmdb_id, schema=schema_name).distinct()
                        )

                    elif t == DASHBOARD_STATS_NUM_TAB:
                        num_dict[t][cmdb_id][schema_name] += ObjTabInfo.\
                            filter_by_exec_hist_id(task_record_id).filter(
                            cmdb_id=cmdb_id,
                            schema_name=schema_name).count()

                    elif t == DASHBOARD_STATS_NUM_INDEX:
                        num_dict[t][cmdb_id][schema_name] += ObjIndColInfo.\
                            filter_by_exec_hist_id(task_record_id).filter(
                            cmdb_id=cmdb_id,
                            schema_name=schema_name).count()

                    elif t == DASHBOARD_STATS_NUM_SEQUENCE:
                        num_dict[t][cmdb_id][schema_name] += ObjSeqInfo.objects(
                            task_record_id=task_record_id,
                            cmdb_id=cmdb_id,
                            schema_name=schema_name).count()
            ret = []
            for t in num_dict:
                for cmdb_id in num_dict[t]:
                    for schema_name in num_dict[t][cmdb_id]:
                        ret.append(cls(
                            drill_down_type=t,
                            cmdb_id=cmdb_id,
                            connect_name=cmdb_id_cmdb_info_dict[cmdb_id],
                            schema_name=schema_name,
                            num=num_dict[t][cmdb_id][schema_name]
                        ))
            return ret




class StatsCMDBOverview(BaseStatisticsDoc):
    """纳管库概览页统计数据"""

    meta = {
        "collection": "stats_cmdb_overview"
    }

    @classmethod
    def generate(cls, task_record_id: int) -> list:
        return []


class StatsCMDBOverviewTabSpace(BaseStatisticsDoc):
    """纳管数据库概览页表空间数据"""

    meta = {
        "collection": "stats_cmdb_overview_tab_space"
    }

    @classmethod
    def generate(cls, task_record_id: int) -> list:
        return []
