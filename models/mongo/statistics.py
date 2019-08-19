# Author: kk.Fang(fkfkbill@gmail.com)

"""
统计信息，用于接口快速获取数据

编码注意：所有涉及sqlalchemy的import，必须在函数内！
"""

from typing import Union

from mongoengine import IntField, StringField, DateTimeField, \
    DynamicField, FloatField, LongField, ListField

from utils.const import *
from .utils import BaseStatisticsDoc


class StatsLoginUser(BaseStatisticsDoc):
    """最后分析：登录用户层面的统计数据，该数据可能根据用户绑定的库以及schema的改变而需要更新。"""

    login_user = StringField(help_text="仅对某个用户有效")
    sql_num = LongField(default=0)
    table_num = IntField(default=0)
    index_num = LongField(default=0)
    sequence_num = IntField(default=0)
    cmdb = ListField(default=lambda: [
        # {
        #     "cmdb_id": "",
        #     "connect_name": "",
        #     "schema_captured_num": 采集的schema个数,
        #     "finally_schema_captured_num": 采集成功的schema个数
        #     "problem_num": {
        #         "SQL": 0,
        #         "OBJ": 0
        #     },
        #     "scores": {
        #         "OBJ": 99,
        #         "TEXT": 99
        #     }
        # }
    ], help_text="分析时该用户的纳管库和纳管schema的统计数据")

    meta = {
        "collection": "stats_login_user"
    }

    @classmethod
    def generate(cls, task_record_id: int, cmdb_id: Union[int, None]):
        from models.oracle import make_session, CMDB, User
        from utils import const
        from utils.score_utils import calc_problem_num, get_result_queryset_by, calc_result, \
            get_latest_task_record_id
        from utils.cmdb_utils import get_current_schema, get_current_cmdb
        from models.mongo.obj import ObjTabInfo, ObjIndColInfo, ObjSeqInfo
        from models.mongo import SQLText

        with make_session() as session:
            for login_user, in session.query(User.login_user):
                doc = cls(task_record_id=task_record_id, login_user=login_user)

                # 计算当前用户绑定的全部库的统计数据
                cmdb_ids = get_current_cmdb(session, login_user)
                latest_task_record_ids = list(
                        get_latest_task_record_id(session, cmdb_ids).values())
                if latest_task_record_ids:
                    doc.sql_num = len(SQLText.filter_by_exec_hist_id(
                        latest_task_record_ids).distinct("sql_id"))
                    doc.table_num = ObjTabInfo.filter_by_exec_hist_id(latest_task_record_ids).count()
                    doc.index_num = ObjIndColInfo.filter_by_exec_hist_id(latest_task_record_ids).count()
                    doc.sequence_num = ObjSeqInfo.objects(
                        cmdb_id__in=cmdb_ids,
                        task_record_id__in=latest_task_record_ids).count()

                # 计算当前用户绑定的各个库的统计数据
                for the_cmdb_id, the_connect_name, the_db_model in \
                        session.query(CMDB.cmdb_id, CMDB.connect_name, CMDB.db_model):
                    if cmdb_id == the_cmdb_id:
                        latest_task_record_id = task_record_id
                    else:
                        latest_task_record_id = get_latest_task_record_id(
                            session, the_cmdb_id).get(the_cmdb_id, None)
                    if not latest_task_record_id:
                        print(f"current latest_task_record_id not exist. cmdb_id = {the_cmdb_id}")
                        continue
                    sql_result_q, _ = get_result_queryset_by(
                        task_record_id=latest_task_record_id,
                        rule_type=const.ALL_RULE_TYPES_FOR_SQL_RULE,
                        cmdb_id=the_cmdb_id
                    )
                    sql_result_score_sum = sum([calc_result(i, the_db_model)[1]
                                                for i in sql_result_q])
                    obj_result_q, _ = get_result_queryset_by(
                        task_record_id=latest_task_record_id,
                        rule_type=const.RULE_TYPE_OBJ,
                        cmdb_id=the_cmdb_id
                    )
                    obj_result_score_sum = sum([calc_result(i, the_db_model)[1]
                                                for i in obj_result_q])
                    schema_captured_num = len(get_current_schema(
                        session, login_user, the_cmdb_id))
                    doc.cmdb.append({
                        "cmdb_id": the_cmdb_id,
                        "connect_name": the_connect_name,
                        "schema_captured_num": schema_captured_num,
                        "finally_schema_captured_num": schema_captured_num,
                        "problem_num": {
                            const.STATS_NUM_SQL: calc_problem_num(sql_result_q),
                            const.RULE_TYPE_OBJ: calc_problem_num(obj_result_q),
                        },
                        "scores": {
                            const.STATS_NUM_SQL: round(sql_result_score_sum / sql_result_q.count(),1)
                            if sql_result_q.count() else 0,
                            const.RULE_TYPE_OBJ: round(obj_result_score_sum / obj_result_q.count(),1)
                            if obj_result_q.count() else 0,
                        }
                    })
                yield doc


class StatsNumDrillDown(BaseStatisticsDoc):
    """results各个维度(rule_type以及obj_info_type)下钻的统计数据"""

    drill_down_type = StringField(choices=ALL_STATS_NUM_TYPE)
    schema_name = StringField()
    job_id = StringField(null=True, help_text="对应的报告job_id")
    num = LongField(default=0, help_text="采集到的总数")
    num_with_risk = LongField(default=0, help_text="有问题的采到的个数")
    num_with_risk_rate = FloatField(help_text="有问题的采到的个数rate")
    problem_num = LongField(default=0, help_text="问题个数")
    problem_num_rate = FloatField(help_text="问题个数rate(风险率)")
    score = FloatField(default=0)

    meta = {
        "collection": "stats_num_drill_down"
    }

    @classmethod
    def generate(cls, task_record_id: int, cmdb_id: Union[int, None]):
        from utils.score_utils import calc_distinct_sql_id, calc_problem_num, \
            get_result_queryset_by
        from models.oracle import make_session, CMDB, RoleDataPrivilege
        from models.mongo import SQLText, MSQLPlan, ObjSeqInfo, ObjTabInfo, \
            ObjIndColInfo, Job
        from utils.cmdb_utils import get_current_schema
        with make_session() as session:
            verbose_schema_info = get_current_schema(
                session,
                cmdb_id=cmdb_id,
                verbose=True,
                query_entity=(CMDB.connect_name, RoleDataPrivilege.schema_name)
            )
            for connect_name, schema_name in set(verbose_schema_info):
                for t in ALL_STATS_NUM_TYPE:
                    # 因为results不同的类型结构不一样，需要分别处理
                    new_doc = cls(
                        task_record_id=task_record_id,
                        drill_down_type=t,
                        cmdb_id=cmdb_id,
                        connect_name=connect_name,
                        schema_name=schema_name,
                    )
                    result_q = None
                    if t == STATS_NUM_SQL_TEXT:
                        new_doc.num = len(
                            SQLText.filter_by_exec_hist_id(task_record_id).
                                filter(cmdb_id=cmdb_id, schema=schema_name).
                                distinct("sql_id")
                        )
                        result_q, _ = get_result_queryset_by(
                            task_record_id=task_record_id,
                            rule_type=RULE_TYPE_TEXT,
                            schema_name=schema_name,
                            cmdb_id=cmdb_id
                        )
                        new_doc.num_with_risk = calc_distinct_sql_id(result_q)
                        new_doc.problem_num = calc_problem_num(result_q)

                    elif t == STATS_NUM_SQL_PLAN:
                        new_doc.num = len(
                            MSQLPlan.filter_by_exec_hist_id(task_record_id).
                                filter(cmdb_id=cmdb_id, schema=schema_name).
                                distinct("plan_hash_value")
                        )
                        result_q, _ = get_result_queryset_by(
                            task_record_id=task_record_id,
                            rule_type=RULE_TYPE_SQLPLAN,
                            schema_name=schema_name,
                            cmdb_id=cmdb_id
                        )
                        new_doc.num_with_risk = calc_distinct_sql_id(result_q)
                        new_doc.problem_num = calc_problem_num(result_q)

                    elif t == STATS_NUM_SQL_STATS:
                        new_doc.num = len(
                            SQLText.filter_by_exec_hist_id(task_record_id).
                                filter(cmdb_id=cmdb_id, schema=schema_name).distinct("sql_id")
                        )
                        result_q, _ = get_result_queryset_by(
                            task_record_id=task_record_id,
                            rule_type=RULE_TYPE_SQLSTAT,
                            schema_name=schema_name,
                            cmdb_id=cmdb_id
                        )
                        new_doc.num_with_risk = calc_distinct_sql_id(result_q)
                        new_doc.problem_num = calc_problem_num(result_q)

                    elif t == STATS_NUM_SQL:
                        new_doc.num = len(
                            SQLText.filter_by_exec_hist_id(task_record_id).
                                filter(cmdb_id=cmdb_id, schema=schema_name).distinct("sql_id")
                        )
                        result_q, _ = get_result_queryset_by(
                            task_record_id=task_record_id,
                            rule_type=ALL_RULE_TYPES_FOR_SQL_RULE,
                            schema_name=schema_name,
                            cmdb_id=cmdb_id
                        )
                        new_doc.num_with_risk = calc_distinct_sql_id(result_q)
                        new_doc.problem_num = calc_problem_num(result_q)

                    elif t == STATS_NUM_TAB:
                        new_doc.num = ObjTabInfo. \
                            filter_by_exec_hist_id(task_record_id).filter(
                            cmdb_id=cmdb_id,
                            schema_name=schema_name).count()
                        result_q, rule_names = get_result_queryset_by(
                            task_record_id=task_record_id,
                            obj_info_type=OBJ_RULE_TYPE_TABLE,
                            schema_name=schema_name,
                            cmdb_id=cmdb_id
                        )
                        new_doc.problem_num = calc_problem_num(result_q, rule_name=rule_names)

                    elif t == STATS_NUM_INDEX:
                        new_doc.num = ObjIndColInfo. \
                            filter_by_exec_hist_id(task_record_id).filter(
                            cmdb_id=cmdb_id,
                            schema_name=schema_name).count()
                        result_q, rule_names = get_result_queryset_by(
                            task_record_id=task_record_id,
                            obj_info_type=OBJ_RULE_TYPE_INDEX,
                            schema_name=schema_name,
                            cmdb_id=cmdb_id
                        )
                        new_doc.problem_num = calc_problem_num(result_q, rule_name=rule_names)

                    elif t == STATS_NUM_SEQUENCE:
                        new_doc.num = ObjSeqInfo.objects(
                            task_record_id=task_record_id,
                            cmdb_id=cmdb_id,
                            schema_name=schema_name).count()
                        result_q, rule_names = get_result_queryset_by(
                            task_record_id=task_record_id,
                            obj_info_type=OBJ_RULE_TYPE_SEQ,
                            schema_name=schema_name,
                            cmdb_id=cmdb_id
                        )
                        new_doc.problem_num = calc_problem_num(result_q, rule_name=rule_names)

                    elif t == STATS_NUM_OBJ:
                        new_doc.num = ObjSeqInfo.objects(
                            task_record_id=task_record_id,
                            cmdb_id=cmdb_id,
                            schema_name=schema_name
                        ).count()
                        new_doc.num += ObjTabInfo.filter_by_exec_hist_id(task_record_id). \
                            filter(
                            cmdb_id=cmdb_id,
                            schema_name=schema_name
                        ).count()
                        new_doc.num += ObjIndColInfo.filter_by_exec_hist_id(task_record_id). \
                            filter(
                            cmdb_id=cmdb_id,
                            schema_name=schema_name
                        ).count()
                        result_q, rule_names = get_result_queryset_by(
                            task_record_id=task_record_id,
                            rule_type=RULE_TYPE_OBJ,
                            schema_name=schema_name,
                            cmdb_id=cmdb_id
                        )
                        new_doc.problem_num = calc_problem_num(result_q, rule_name=rule_names)

                    if new_doc.num:
                        # 有问题个数/总个数
                        new_doc.num_with_risk_rate = new_doc.num_with_risk / new_doc.num
                        # 风险率
                        new_doc.problem_num_rate = new_doc.problem_num / new_doc.num
                    if result_q:
                        r = result_q.first()
                        j = Job.objects(id=r.task_uuid).first()
                        new_doc.job_id = str(j.id)
                        new_doc.score = j.score
                    yield new_doc


class StatsCMDBPhySize(BaseStatisticsDoc):
    """概览页库容量"""

    total = FloatField(help_text="bytes")
    free = FloatField(help_text="bytes")
    used = FloatField(help_text="bytes")
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
            for ts in ObjTabSpace. \
                    objects(task_record_id=task_record_id, cmdb_id=cmdb_id).all():
                doc.total += ts.total
                doc.free += ts.free
                doc.used += ts.used
            doc.usage_ratio = doc.used / doc.total
            yield doc
