# Author: kk.Fang(fkfkbill@gmail.com)

"""
统计信息，用于接口快速获取数据

编码注意：所有涉及sqlalchemy的import，必须在函数内！
"""

from typing import Union
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
    def generate(cls, task_record_id: int, cmdb_id: Union[int, None]) -> list:
        return []


class StatsDashboardDrillDown(BaseStatisticsDoc):
    """仪表盘四个数据的下钻"""

    drill_down_type = StringField(choices=ALL_DASHBOARD_STATS_NUM_TYPE)
    connect_name = StringField()
    schema_name = StringField()
    num = LongField()

    meta = {
        "collection": "stats_dashboard_drill_down"
    }

    @classmethod
    def generate(cls, task_record_id: int, cmdb_id: Union[int, None]) -> list:
        from utils.score_utils import get_latest_task_record_id
        from models.oracle import make_session, DataPrivilege, CMDB, QueryEntity
        from models.mongo import SQLText, ObjSeqInfo, ObjTabInfo, ObjIndColInfo
        ret = []
        with make_session() as session:
            # the type: cmdb_id: schema_name: num
            num_dict = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: 0)))
            qe = QueryEntity(
                CMDB.cmdb_id,
                CMDB.connect_name,
                DataPrivilege.schema_name
            )
            rst = session.query(*qe).join(CMDB, DataPrivilege.cmdb_id == CMDB.cmdb_id)
            cmdb_id_cmdb_info_dict = {i["cmdb_id"]: i for i in [qe.to_dict(j) for j in rst]}
            latest_task_record_id_dict = get_latest_task_record_id(
                session, list(cmdb_id_cmdb_info_dict.keys()))
            for cmdb_id, connect_name, schema_name in set(rst):  # 必须去重
                latest_task_record_id = latest_task_record_id_dict[cmdb_id]
                for t in ALL_DASHBOARD_STATS_NUM_TYPE:
                    # 因为results不同的类型结构不一样，需要分别处理
                    if t == DASHBOARD_STATS_NUM_SQL:
                        num_dict[t][cmdb_id][schema_name] += len(
                            SQLText.filter_by_exec_hist_id(latest_task_record_id).
                            filter(cmdb_id=cmdb_id, schema=schema_name).distinct("sql_id")
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
            for t in num_dict:
                for cmdb_id in num_dict[t]:
                    for schema_name in num_dict[t][cmdb_id]:
                        ret.append(cls(
                            task_record_id=task_record_id,
                            drill_down_type=t,
                            cmdb_id=cmdb_id,
                            connect_name=cmdb_id_cmdb_info_dict[cmdb_id],
                            schema_name=schema_name,
                            num=num_dict[t][cmdb_id][schema_name]
                        ))
        return ret


class StatsCMDBPhySize(BaseStatisticsDoc):
    """概览页库容量"""

    connect_name = StringField()
    total = LongField(help_text="bytes")
    free = LongField(help_text="bytes")
    used = LongField(help_text="bytes")
    usage_ratio = FloatField()

    meta = {
        "collection": "stats_cmdb_phy_size"
    }

    @classmethod
    def generate(cls, task_record_id: int, cmdb_id: Union[int, None]) -> list:
        from models.oracle import make_session, CMDB, QueryEntity
        from models.mongo import ObjTabSpace
        ret = []
        with make_session() as session:
            qe = QueryEntity(CMDB.cmdb_id, CMDB.connect_name)
            for cmdb_info in session.query(*qe):
                cmdb_dict = qe.to_dict(cmdb_info)
                cmdb_id = cmdb_dict["cmdb_id"]
                doc = cls(
                    task_record_id=task_record_id,
                    cmdb_id=cmdb_id,
                    total=0,
                    free=0,
                    used=0,
                    usage_ratio=0
                )
                for ts in ObjTabSpace.\
                        objects(task_record_id=task_record_id, cmdb_id=cmdb_id).all():
                    doc.total += ts.total
                    doc.free += ts.free
                    doc.used += ts.used
                doc.usage_ratio = doc.used / doc.total
                ret.append(doc)
        return ret
