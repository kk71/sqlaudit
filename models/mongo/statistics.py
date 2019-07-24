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
    def generate(cls, task_record_id: int):
        return


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
    def generate(cls, task_record_id: int):
        from utils.score_utils import get_latest_task_record_id, get_result_object_by_type
        from models.oracle import make_session, DataPrivilege, CMDB, QueryEntity

        with make_session() as session:
            # cmdb_id: schema_name: type: set()
            # 这里的set存放的是对象的唯一标识
            # 唯一标识: 对象是get_key()，SQL是sql_id
            set_dict = defaultdict(lambda: defaultdict(lambda: defaultdict(set)))
            qe = QueryEntity([
                CMDB.cmdb_id,
                CMDB.connect_name,
                DataPrivilege.schema_name
            ])
            rst = session.query(*qe).join(CMDB, DataPrivilege.cmdb_id == CMDB.cmdb_id)
            cmdb_ids = [i["cmdb_id"] for i in qe.to_dict(rst)]
            latest_task_record_id_dict = get_latest_task_record_id(session, cmdb_ids)
            for cmdb_id, connect_name, schema_name in set(rst):  # 必须去重
                latest_task_record_id = latest_task_record_id_dict[cmdb_id]
                for t in ALL_DASHBOARD_STATS_NUM_TYPE:
                    # 因为results不同的类型结构不一样，需要分别处理
                    if t == DASHBOARD_STATS_NUM_SQL:
                        result_q, rule_names = get_result_object_by_type(
                            latest_task_record_id, rule_type=ALL_RULE_TYPES_FOR_SQL_RULE)
                        for result in result_q:
                            for rn in rule_names:
                                result_rule_dict = getattr(result, rn, None)
                                if not result_rule_dict:
                                    continue
                                set_dict[cmdb_id][schema_name][t].update([
                                    i["sql_id"] for i in result_rule_dict["sqls"]
                                ])

                    else:
                        result_q, rule_names = get_result_object_by_type(
                            latest_task_record_id, rule_type=RULE_TYPE_OBJ, obj_info_type=t)
                        for result in result_q:
                            for rn in rule_names:
                                result_rule_dict = getattr(result, rn, None)
                                if not result_rule_dict:
                                    continue
                                set_dict[cmdb_id][schema_name][t].update([
                                    
                                ])



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
