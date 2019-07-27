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
    def generate(cls, task_record_id: int, cmdb_id: Union[int, None]):
        return


class StatsNumDrillDown(BaseStatisticsDoc):
    """results各个维度(rule_type以及obj_info_type)下钻的统计数据"""

    drill_down_type = StringField(choices=ALL_STATS_NUM_TYPE)
    schema_name = StringField()
    num = LongField(default=0, help_text="采集到的总数")
    num_with_risk = LongField(default=0, help_text="有问题的采到的个数")
    num_with_risk_rate = FloatField(help_text="有问题的采到的个数rate")
    problem_num = LongField(default=0, help_text="问题个数")
    problem_num_rate = FloatField(help_text="问题个数rate(风险率)")

    meta = {
        "collection": "stats_num_drill_down"
    }

    @classmethod
    def generate(cls, task_record_id: int, cmdb_id: Union[int, None]):
        from utils.score_utils import get_latest_task_record_id, calc_distinct_sql_id, \
            calc_problem_num, get_result_queryset_by_type
        from models.oracle import make_session, DataPrivilege, CMDB, QueryEntity
        from models.mongo import SQLText, SQLStat, MSQLPlan, ObjSeqInfo, ObjTabInfo, \
            ObjIndColInfo
        with make_session() as session:
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
                for t in ALL_STATS_NUM_TYPE:
                    # 因为results不同的类型结构不一样，需要分别处理
                    new_doc = cls(
                        task_record_id=task_record_id,
                        drill_down_type=t,
                        cmdb_id=cmdb_id,
                        connect_name=cmdb_id_cmdb_info_dict[cmdb_id]["connect_name"],
                        schema_name=schema_name,
                    )
                    if t == STATS_NUM_SQL_TEXT:
                        new_doc.num = len(
                            SQLText.filter_by_exec_hist_id(latest_task_record_id).
                                filter(cmdb_id=cmdb_id, schema=schema_name).distinct("sql_id")
                        )
                        result_q, _ = get_result_queryset_by_type(
                            task_record_id=task_record_id,
                            rule_type=RULE_TYPE_TEXT
                        )
                        new_doc.num_with_risk = calc_distinct_sql_id(result_q)

                    elif t == STATS_NUM_SQL_PLAN:
                        new_doc.num = len(
                            MSQLPlan.filter_by_exec_hist_id(latest_task_record_id).
                                filter(cmdb_id=cmdb_id, schema=schema_name).
                                distinct("plan_hash_value")
                        )
                        result_q, _ = get_result_queryset_by_type(
                            task_record_id=task_record_id,
                            rule_type=RULE_TYPE_SQLPLAN
                        )
                        new_doc.num_with_risk = calc_distinct_sql_id(result_q)

                    elif t == STATS_NUM_SQL_STATS:
                        new_doc.num = len(
                            SQLText.filter_by_exec_hist_id(latest_task_record_id).
                                filter(cmdb_id=cmdb_id, schema=schema_name).distinct("sql_id")
                        )
                        result_q, _ = get_result_queryset_by_type(
                            task_record_id=task_record_id,
                            rule_type=RULE_TYPE_SQLSTAT
                        )
                        new_doc.num_with_risk = calc_distinct_sql_id(result_q)

                    elif t == STATS_NUM_SQL:
                        new_doc.num = len(
                            SQLText.filter_by_exec_hist_id(latest_task_record_id).
                            filter(cmdb_id=cmdb_id, schema=schema_name).distinct("sql_id")
                        )
                        result_q, _ = get_result_queryset_by_type(
                            task_record_id=task_record_id,
                            rule_type=ALL_RULE_TYPES_FOR_SQL_RULE
                        )
                        new_doc.num_with_risk = calc_distinct_sql_id(result_q)

                    elif t == STATS_NUM_TAB:
                        new_doc.num = ObjTabInfo.\
                            filter_by_exec_hist_id(task_record_id).filter(
                            cmdb_id=cmdb_id,
                            schema_name=schema_name).count()
                        result_q, rule_names = get_result_queryset_by_type(
                            task_record_id=task_record_id,
                            rule_type=RULE_TYPE_OBJ,
                            obj_info_type=OBJ_RULE_TYPE_TABLE
                        )
                        new_doc.problem_num = calc_problem_num(result_q, rule_name=rule_names)

                    elif t == STATS_NUM_INDEX:
                        new_doc.num = ObjIndColInfo.\
                            filter_by_exec_hist_id(task_record_id).filter(
                            cmdb_id=cmdb_id,
                            schema_name=schema_name).count()
                        result_q, rule_names = get_result_queryset_by_type(
                            task_record_id=task_record_id,
                            rule_type=RULE_TYPE_OBJ,
                            obj_info_type=OBJ_RULE_TYPE_INDEX
                        )
                        new_doc.problem_num = calc_problem_num(result_q, rule_name=rule_names)

                    elif t == STATS_NUM_SEQUENCE:
                        new_doc.num = ObjSeqInfo.objects(
                            task_record_id=task_record_id,
                            cmdb_id=cmdb_id,
                            schema_name=schema_name).count()
                        result_q, rule_names = get_result_queryset_by_type(
                            task_record_id=task_record_id,
                            rule_type=RULE_TYPE_OBJ,
                            obj_info_type=OBJ_RULE_TYPE_SEQ
                        )
                        new_doc.problem_num = calc_problem_num(result_q, rule_name=rule_names)

                    if new_doc.num:
                        # 有问题个数/总个数
                        new_doc.num_with_risk_rate = new_doc.num_with_risk / new_doc.num
                        # 风险率
                        new_doc.problem_num_rate = new_doc.problem_num / new_doc.num
                    yield new_doc


class StatsCMDBPhySize(BaseStatisticsDoc):
    """概览页库容量"""

    total = LongField(help_text="bytes")
    free = LongField(help_text="bytes")
    used = LongField(help_text="bytes")
    usage_ratio = FloatField()

    meta = {
        "collection": "stats_cmdb_phy_size"
    }

    @classmethod
    def generate(cls, task_record_id: int, cmdb_id: Union[int, None]):
        from models.oracle import make_session, CMDB
        from models.mongo import ObjTabSpace
        with make_session() as session:
            cmdb = session.query(CMDB).filter(CMDB.cmdb_id == cmdb_id).first()
            doc = cls(
                task_record_id=task_record_id,
                cmdb_id=cmdb_id,
                connect_name=cmdb.connect_name,
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
            yield doc


class StatsSQLObject(BaseStatisticsDoc):
    """统计出sql和对象的个数"""

    schema_name = StringField()

    active_sql_num = LongField()
    risk_sql_num = LongField(help_text="问题SQL个数")
    sql_problem_num = LongField(help_text="SQL问题数")

    active_object_num = LongField()
    object_problem_num = LongField(help_text="对象问题数")

    meta = {
        "collection": "stats_sql_object"
    }

    @classmethod
    def generate(cls, task_record_id: int, cmdb_id: Union[int, None]):
        from models.mongo import (
            Results,
            SQLText,
            ObjTabInfo,
            ObjSeqInfo,
            ObjViewInfo,
            ObjIndColInfo
        )
        from models.oracle import make_session, CMDB, DataPrivilege
        from utils import sql_utils
        with make_session() as session:
            schemas_set = {i[0] for i in session.query(DataPrivilege.schema_name).
                                                    filter_by(cmdb_id=cmdb_id)}
            for schema_name in schemas_set:
                m = cls(schema_name=schema_name)
                m.active_sql_num = len(SQLText.filter_by_exec_hist_id(task_record_id).
                                       distinct("sql_id"))
                m.risk_sql_num = len(sql_utils.get_risk_sql_list(
                    cmdb_id=cmdb_id,
                    date_range=(None, None),
                    schema_name=schema_name,
                    sql_id_only=True,
                    sqltext_stats=False,
                    task_record_id=task_record_id
                ))
                yield m
