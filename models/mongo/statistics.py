# Author: kk.Fang(fkfkbill@gmail.com)

"""
统计信息，用于接口快速获取数据

编码注意：所有涉及sqlalchemy的import，必须在函数内！
"""

from typing import Union
from collections import defaultdict

from mongoengine import IntField, StringField, DateTimeField, \
    DynamicField, FloatField, LongField, ListField, DictField, EmbeddedDocumentField, \
    EmbeddedDocument, EmbeddedDocumentListField

from utils.const import *
from .utils import BaseStatisticsDoc


class StatsLoginUser_SchemaRank(EmbeddedDocument):
    """
    纳管库的schema分数排名
    """
    schema_name = StringField()
    connect_name = StringField()
    health_score = FloatField()
    collect_date = DateTimeField()


class StatsLoginUser_TablespaceRank(EmbeddedDocument):
    """
    纳管库的表空间使用率排名
    """
    tablespace_name = StringField()
    usage_ratio = FloatField()
    cmdb_id = IntField()
    connect_name = StringField()


class StatsLoginUser_CMDB(EmbeddedDocument):
    """
    当前登录用户各个纳管数据库的统计信息
    """
    cmdb_id = IntField()
    connect_name = StringField()
    schema_captured_num = FloatField()
    finally_schema_captured_num = FloatField()
    problem_num = DictField(default=lambda: {"SQL": 0, "OBJ": 0})
    scores = DictField(default=lambda: {"OBJ": 0, "TEXT": 0})


class StatsLoginUser(BaseStatisticsDoc):
    """最后分析：登录用户层面的统计数据，该数据可能根据用户绑定的库以及schema的改变而需要更新。"""

    login_user = StringField(help_text="仅对某个用户有效")

    # SQL
    sql_num = LongField(default=0)  # 采集到的sql总数（按照sql_id去重）
    sql_problem_num = LongField(default=0)  # 包含问题的sql数（去重）
    sql_problem_rate = FloatField(default=0.0)  # sql_problem_num/sql_num
    problem_num_of_sql = LongField(default=0)  # sql的问题数

    # TABLE
    table_num = IntField(default=0)  # 采集到的表数
    table_problem_num = IntField(default=0)  # 包含问题的表数目
    table_problem_rate = FloatField(default=0.0)  # table_problem_num/table_num
    problem_num_of_table = LongField(default=0)  # table的问题数

    # INDEX
    index_num = LongField(default=0)  # 采集到的索引数
    index_problem_num = LongField(default=0)  # 包含问题的索引数
    index_problem_rate = FloatField(default=0.0)  # index_problem_num/index_num
    problem_num_of_index = LongField(default=0)  # index的问题数

    # SEQUENCE
    sequence_num = IntField(default=0)  # 采集到的序列数
    sequence_problem_num = IntField(default=0)  # 包含问题的序列数
    sequence_problem_rate = FloatField(default=0.0)  # sequence_problem_num/sequence_num
    problem_num_of_sequence = LongField(default=0)  # sequence的问题数

    schema_rank = EmbeddedDocumentListField(StatsLoginUser_SchemaRank, default=list)
    tablespace_rank = EmbeddedDocumentListField(StatsLoginUser_TablespaceRank, default=list)
    cmdb = EmbeddedDocumentListField(StatsLoginUser_CMDB, default=list)

    meta = {
        "collection": "stats_login_user",
        "indexes": ["login_user"]
    }

    @classmethod
    def generate(cls, task_record_id: int, cmdb_id: Union[int, None]):

        # =====================================
        # === 这个统计依赖于 StatsNumDrillDown ===
        # =====================================

        from mongoengine import Q

        from models.oracle import make_session, CMDB, User
        from utils import const
        from utils.score_utils import calc_problem_num, get_result_queryset_by, calc_result, \
            get_latest_task_record_id, calc_score_by, calc_distinct_sql_id, \
            get_object_unique_labels
        from utils.cmdb_utils import get_current_schema, get_current_cmdb
        from models.mongo.obj import ObjTabInfo, ObjIndColInfo, ObjSeqInfo, ObjTabSpace

        with make_session() as session:
            for login_user, in session.query(User.login_user):
                doc = cls(task_record_id=task_record_id, login_user=login_user)

                # 计算当前用户绑定的全部库的统计数据
                cmdb_ids = get_current_cmdb(session, login_user)
                cmdb_ids_schemas_dict = {
                    cmdb_id: get_current_schema(session, login_user, cmdb_id)
                    for cmdb_id in cmdb_ids
                }
                cmdb_id_connect_names_pair: dict = dict(session.query(
                    CMDB.cmdb_id, CMDB.connect_name))
                latest_task_record_ids = list(
                    get_latest_task_record_id(
                        session,
                        cmdb_ids,
                        task_record_id_to_replace={cmdb_id: task_record_id}
                    ).values()
                )
                print("latest task record ids(current included) of the user "
                      f"{login_user}: {latest_task_record_ids}")
                if latest_task_record_ids:

                    Qs = None
                    for cmdb_id, schema_list in cmdb_ids_schemas_dict.items():
                        a_q = Q(cmdb_id=cmdb_id, schema_name__in=schema_list)
                        if not Qs:
                            Qs = a_q
                        else:
                            Qs = Qs | a_q

                    # SQL ==============
                    if Qs:
                        stats_num_drill_down_q = StatsNumDrillDown.objects(
                            Qs,
                            drill_down_type=STATS_NUM_SQL,
                            task_record_id__in=latest_task_record_ids
                        )
                        for a_drill_down in stats_num_drill_down_q:
                            doc.sql_num += a_drill_down.num
                            doc.sql_problem_num += a_drill_down.num_with_risk
                            doc.problem_num_of_sql += a_drill_down.problem_num

                    if doc.sql_num:
                        doc.sql_problem_rate = round(doc.sql_problem_num / float(doc.sql_num), 4)

                    # TABLE ============
                    if Qs:
                        stats_num_drill_down_q = StatsNumDrillDown.objects(
                            Qs,
                            drill_down_type=STATS_NUM_TAB,
                            task_record_id__in=latest_task_record_ids
                        )
                        for a_drill_down in stats_num_drill_down_q:
                            doc.table_num += a_drill_down.num
                            doc.table_problem_num += a_drill_down.num_with_risk
                            doc.problem_num_of_table += a_drill_down.problem_num
                    if doc.table_num:
                        doc.table_problem_rate = round(doc.table_problem_num / float(doc.table_num), 4)

                    # INDEX =============
                    if Qs:
                        stats_num_drill_down_q = StatsNumDrillDown.objects(
                            Qs,
                            drill_down_type=STATS_NUM_INDEX,
                            task_record_id__in=latest_task_record_ids
                        )
                        for a_drill_down in stats_num_drill_down_q:
                            doc.index_num += a_drill_down.num
                            doc.index_problem_num += a_drill_down.num_with_risk
                            doc.problem_num_of_index += a_drill_down.problem_num
                    if doc.index_num:
                        doc.index_problem_rate = round(doc.index_problem_num / float(doc.index_num), 4)

                    # SEQUENCE ===========
                    if Qs:
                        stats_num_drill_down_q = StatsNumDrillDown.objects(
                            Qs,
                            drill_down_type=STATS_NUM_SEQUENCE,
                            task_record_id__in=latest_task_record_ids
                        )
                        for a_drill_down in stats_num_drill_down_q:
                            doc.sequence_num += a_drill_down.num
                            doc.sequence_problem_num += a_drill_down.num_with_risk
                            doc.problem_num_of_sequence += a_drill_down.problem_num
                    if doc.sequence_num:
                        doc.sequence_problem_rate = round(doc.sequence_problem_num / float(doc.sequence_num), 4)

                    # schema排名
                    tab_space = ObjTabSpace.objects(task_record_id__in=latest_task_record_ids). \
                                    order_by("-usage_ratio")[:10]
                    for ts in tab_space:
                        doc.tablespace_rank.append(StatsLoginUser_TablespaceRank(
                            **ts.to_dict(iter_if=lambda k, v: k in (
                                "tablespace_name", "usage_ratio", "cmdb_id")),
                            connect_name=cmdb_id_connect_names_pair.get(ts.cmdb_id, None)
                        ))

                    # schema分数排名
                    all_current_cmdb_schema_dict = dict()
                    for the_cmdb in session.query(CMDB).filter(CMDB.cmdb_id.in_(cmdb_ids)):
                        for the_schema, the_score in calc_score_by(
                                session,
                                the_cmdb,
                                perspective=const.OVERVIEW_ITEM_SCHEMA,
                                score_by=const.SCORE_BY_LOWEST
                        ).items():
                            all_current_cmdb_schema_dict[(the_cmdb.cmdb_id, the_schema)] = \
                                StatsLoginUser_SchemaRank(
                                    schema_name=the_schema,
                                    connect_name=the_cmdb.connect_name,
                                    health_score=the_score,
                                    collect_date=None
                                )
                    doc.schema_rank = list(dict(sorted(
                        all_current_cmdb_schema_dict.items(),
                        key=lambda x: x[1].health_score
                    )[:10]).values())  # 只取分数最低的x个

                # 计算当前用户绑定的各个库的统计数据
                for the_cmdb_id, the_connect_name, the_db_model in \
                        session.query(CMDB.cmdb_id, CMDB.connect_name, CMDB.db_model). \
                                filter(CMDB.cmdb_id.in_(cmdb_ids)):
                    if cmdb_id == the_cmdb_id:
                        latest_task_record_id = task_record_id
                    else:
                        latest_task_record_id = get_latest_task_record_id(
                            session, the_cmdb_id).get(the_cmdb_id, None)
                    if not latest_task_record_id:
                        print(f"current latest_task_record_id not exist. cmdb_id = {the_cmdb_id}")
                        continue
                    print(f"for {login_user}, cmdb_id={the_cmdb_id} has"
                          f" lastest_task_record_id={latest_task_record_id}")
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
                    if sql_result_q.count():
                        score_sql = round(sql_result_score_sum / sql_result_q.count(), 1)
                        if not sql_result_score_sum:
                            print("sql_result_score_sum  == 0")
                    else:
                        print("no sql result was found.")
                        score_sql = 0
                    if obj_result_q.count():
                        score_obj = round(obj_result_score_sum / obj_result_q.count(), 1)
                        if not obj_result_score_sum:
                            print("obj_result_score_sum == 0")
                    else:
                        print("no obj result was found.")
                        score_obj = 0
                    doc.cmdb.append(StatsLoginUser_CMDB(
                        cmdb_id=the_cmdb_id,
                        connect_name=the_connect_name,
                        schema_captured_num=schema_captured_num,
                        finally_schema_captured_num=schema_captured_num,
                        problem_num={
                            const.STATS_NUM_SQL: calc_problem_num(sql_result_q),
                            const.RULE_TYPE_OBJ: calc_problem_num(obj_result_q),
                        },
                        scores={
                            const.STATS_NUM_SQL: score_sql,
                            const.RULE_TYPE_OBJ: score_obj,
                        }
                    ))
                yield doc


class StatsCMDBLoginUser(BaseStatisticsDoc):
    """登录用户所绑定的库的统计信息"""

    DATE_PERIOD = (7, 30)  # 数据日期范围

    login_user = StringField(help_text="用户")
    date_period = IntField(help_text="时间区间", choices=DATE_PERIOD)
    sql_num = DictField(default=lambda: {"active": [], "at_risk": []})
    risk_rule_rank = DictField(default=lambda:
    {
        "rule_name": None,
        "num": 0,
        "risk_name": None,
        "severity": None,
    })
    sql_execution_cost_rank = DictField(default=lambda: {"by_sum": [], "by_average": []})
    risk_rate = DictField(default=dict)

    meta = {
        "collection": "stats_cmdb_login_user",
        "indexes": ["login_user"]
    }

    @classmethod
    def generate(cls, task_record_id: int, cmdb_id: Union[int, None]):
        from copy import deepcopy
        import arrow
        from models.oracle import make_session, User
        from models.mongo import SQLText, Results
        from utils.cmdb_utils import get_current_schema
        from utils.sql_utils import get_risk_sql_list
        from utils.datetime_utils import dt_to_str, date
        from utils.rule_utils import get_risk_rules_dict, get_risk_rate

        arrow_now = arrow.now()

        with make_session() as session:
            for login_user, in session.query(User.login_user):
                schemas = get_current_schema(session, login_user, cmdb_id)
                for dp in cls.DATE_PERIOD:
                    doc = cls(
                        task_record_id=task_record_id,
                        cmdb_id=cmdb_id,
                        login_user=login_user,
                        date_period=dp
                    )
                    date_start: date = arrow_now.shift(days=-dp).date()
                    date_end: date = arrow_now.shift(days=1).date()
                    dt_now = deepcopy(arrow_now).shift(days=-dp)

                    # SQL count
                    while dt_now.date() < date_end:
                        sql_text_q = SQLText.objects(
                            cmdb_id=cmdb_id,
                            etl_date__gte=dt_now.date(),
                            etl_date__lt=dt_now.shift(days=+1).date(),
                            schema__in=schemas
                        )
                        active_sql_num = len(sql_text_q.distinct("sql_id"))
                        at_risk_sql_num = len(get_risk_sql_list(
                            session=session,
                            cmdb_id=cmdb_id,
                            # schema_name=schema_name,
                            sql_id_only=True,
                            date_range=(dt_now.date(), dt_now.shift(days=+1).date())
                        ))
                        doc.sql_num["active"].append({
                            "date": dt_to_str(dt_now),
                            "value": active_sql_num
                        })
                        doc.sql_num["at_risk"].append({
                            "date": dt_to_str(dt_now),
                            "value": at_risk_sql_num
                        })
                        dt_now = dt_now.shift(days=+1)

                    # risk_rule_rank

                    # 只需要拿到rule_name即可，不需要知道其他两个key,
                    # 因为当前仅对某一个库做分析，数据库类型和db_model都是确定的
                    risk_rule_name_sql_num_dict = {
                        # rule_name: {...}
                        r3key[2]: {
                            "violation_num": 0,
                            "schema_set": set(),
                            **robj.to_dict(iter_if=lambda k, v: k in ("risk_name", "severity"))
                        }
                        for r3key, robj in get_risk_rules_dict(session).items()}
                    results_q = Results.objects(
                        cmdb_id=cmdb_id,
                        create_date__gte=date_start,
                        create_date__lte=date_end,
                        schema_name__in=schemas
                    )
                    for result in results_q:
                        for rule_name in risk_rule_name_sql_num_dict.keys():
                            result_rule_dict = getattr(result, rule_name, None)
                            if not result_rule_dict:
                                continue
                            if result_rule_dict.get("records", []) or result_rule_dict.get("sqls", []):
                                risk_rule_name_sql_num_dict[rule_name]["violation_num"] += 1
                                risk_rule_name_sql_num_dict[rule_name]["schema_set"]. \
                                    add(result.schema_name)
                    doc.risk_rule_rank = [
                        {
                            "rule_name": rule_name,
                            "num": k["violation_num"],
                            "risk_name": k["risk_name"],
                            "severity": k["severity"],
                        } for rule_name, k in risk_rule_name_sql_num_dict.items()
                    ]

                    doc.risk_rule_rank = sorted(doc.risk_rule_rank, key=lambda x: x["num"], reverse=True)

                    # top 10 execution cost by sum and by average
                    sqls = get_risk_sql_list(
                        session=session,
                        cmdb_id=cmdb_id,
                        # schema_name=schema_name,
                        date_range=(date_start, date_end),
                        sqltext_stats=False
                    )
                    sql_by_sum = [
                        {"sql_id": sql["sql_id"], "time": sql["execution_time_cost_sum"]}
                        for sql in sqls
                    ]
                    top_10_sql_by_sum = sorted(
                        sql_by_sum,
                        key=lambda x: x["time"],
                        reverse=True
                    )[:10]
                    top_10_sql_by_sum.reverse()
                    sql_by_average = [
                        {"sql_id": sql["sql_id"], "time": sql["execution_time_cost_on_average"]}
                        for sql in sqls
                    ]
                    top_10_sql_by_average = sorted(
                        sql_by_average,
                        key=lambda x: x["time"],
                        reverse=True
                    )[:10]
                    top_10_sql_by_average.reverse()
                    doc.sql_execution_cost_rank["by_sum"] = top_10_sql_by_sum
                    doc.sql_execution_cost_rank["by_average"] = top_10_sql_by_average
                    doc.risk_rate = get_risk_rate(
                        session=session,
                        cmdb_id=cmdb_id,
                        date_range=(date_start, date_end)
                    )
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
            get_result_queryset_by, get_object_unique_labels
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
                        result_q, rule_names_text = get_result_queryset_by(
                            task_record_id=task_record_id,
                            rule_type=RULE_TYPE_TEXT,
                            schema_name=schema_name,
                            cmdb_id=cmdb_id
                        )
                        new_doc.num_with_risk = calc_distinct_sql_id(result_q)
                        new_doc.problem_num = calc_problem_num(
                            result_q, rule_name=rule_names_text)

                    elif t == STATS_NUM_SQL_PLAN:
                        new_doc.num = len(
                            MSQLPlan.filter_by_exec_hist_id(task_record_id).
                                filter(cmdb_id=cmdb_id, schema=schema_name).
                                distinct("plan_hash_value")
                        )
                        result_q, rule_names_sqlplan = get_result_queryset_by(
                            task_record_id=task_record_id,
                            rule_type=RULE_TYPE_SQLPLAN,
                            schema_name=schema_name,
                            cmdb_id=cmdb_id
                        )
                        new_doc.num_with_risk = calc_distinct_sql_id(result_q)
                        new_doc.problem_num = calc_problem_num(
                            result_q, rule_name=rule_names_sqlplan)

                    elif t == STATS_NUM_SQL_STATS:
                        new_doc.num = len(
                            SQLText.filter_by_exec_hist_id(task_record_id).
                                filter(cmdb_id=cmdb_id, schema=schema_name).distinct("sql_id")
                        )
                        result_q, rule_names_sqlstat = get_result_queryset_by(
                            task_record_id=task_record_id,
                            rule_type=RULE_TYPE_SQLSTAT,
                            schema_name=schema_name,
                            cmdb_id=cmdb_id
                        )
                        new_doc.num_with_risk = calc_distinct_sql_id(result_q)
                        new_doc.problem_num = calc_problem_num(
                            result_q, rule_name=rule_names_sqlstat)

                    elif t == STATS_NUM_SQL:
                        new_doc.num = len(
                            SQLText.filter_by_exec_hist_id(task_record_id).
                                filter(cmdb_id=cmdb_id, schema=schema_name).distinct("sql_id")
                        )
                        result_q, rule_names_sql = get_result_queryset_by(
                            task_record_id=task_record_id,
                            rule_type=ALL_RULE_TYPES_FOR_SQL_RULE,
                            schema_name=schema_name,
                            cmdb_id=cmdb_id
                        )
                        new_doc.num_with_risk = calc_distinct_sql_id(result_q)
                        new_doc.problem_num = calc_problem_num(
                            result_q,
                            rule_name=rule_names_sql
                        )

                    elif t == STATS_NUM_TAB:
                        # 统计个数的时候记得去重！
                        new_doc.num = len(
                            ObjTabInfo.filter_by_exec_hist_id(task_record_id).filter(
                                cmdb_id=cmdb_id,
                                schema_name=schema_name
                            ).distinct("table_name")
                        )
                        result_q, rule_names = get_result_queryset_by(
                            task_record_id=task_record_id,
                            obj_info_type=OBJ_RULE_TYPE_TABLE,
                            schema_name=schema_name,
                            cmdb_id=cmdb_id
                        )
                        new_doc.num_with_risk = len(get_object_unique_labels(
                            result_q, rule_names))
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
                        new_doc.num_with_risk = len(get_object_unique_labels(
                            result_q, rule_names))
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
                        new_doc.num_with_risk = len(get_object_unique_labels(
                            result_q, rule_names))
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
                        new_doc.num_with_risk = len(get_object_unique_labels(
                            result_q, rule_names))
                        new_doc.problem_num = calc_problem_num(result_q, rule_name=rule_names)

                    if new_doc.num_with_risk > new_doc.num:
                        # 如果发现有问题的对象数大于实际数，则修正为实际数
                        new_doc.num_with_risk = new_doc.num
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


class StatsRiskSqlRule(BaseStatisticsDoc):
    """风险sql外层规则"""

    rule = DictField(default=dict)
    severity = StringField()
    last_appearance = DateTimeField()
    rule_num = IntField(default=0, help_text="该规则找到的触犯数")
    schema = StringField(null=False)

    meta = {
        "collection": "stats_risk_sql_rule"
    }

    @classmethod
    def generate(cls, task_record_id: int, cmdb_id: Union[int, None]):
        import arrow
        from utils.sql_utils import get_risk_sql_list
        from utils.cmdb_utils import get_current_schema
        from models.oracle import make_session
        from collections import defaultdict

        with make_session() as session:
            rst = get_risk_sql_list(
                session=session,
                cmdb_id=cmdb_id,
                date_range=(None, None),
                task_record_id=task_record_id,
                task_record_id_to_replace={cmdb_id: task_record_id}
            )

        # rule_name, schema:
        rsts = defaultdict(lambda: defaultdict(cls))

        for x in rst:
            doc = rsts[x["rule"]["rule_name"]][x['schema']]
            doc.task_record_id = task_record_id
            doc.cmdb_id = cmdb_id
            doc.rule = x["rule"]
            doc.severity = x["severity"]
            doc.last_appearance = arrow.get(x["last_appearance"]).datetime
            doc.schema = x['schema']
            doc.rule_num += 1
        for i in rsts.values():
            for j in i.values():
                yield j


class StatsRiskObjectsRule(BaseStatisticsDoc):
    """风险对象外层规则"""

    rule = DictField(default=dict)
    severity = StringField()
    last_appearance = DateTimeField()
    rule_num = IntField(default=0, help_text="该规则找到的触犯数")
    schema = StringField(null=False)

    meta = {
        "collection": "stats_risk_objects_rule"
    }

    @classmethod
    def generate(cls, task_record_id: int, cmdb_id: Union[int, None]):
        import arrow
        from utils.object_utils import get_risk_object_list
        from utils.cmdb_utils import get_current_schema
        from models.oracle import make_session
        from collections import defaultdict

        with make_session() as session:
            rst = get_risk_object_list(
                session=session,
                cmdb_id=cmdb_id,
                task_record_id=task_record_id
            )
        # rule_name, schema:
        rsts = defaultdict(lambda: defaultdict(cls))

        for x in rst:
            doc = rsts[x["rule"]["rule_name"]][x['schema']]
            doc.task_record_id = task_record_id
            doc.cmdb_id = cmdb_id
            doc.rule = x["rule"]
            doc.severity = x["severity"]
            doc.last_appearance = arrow.get(x["last_appearance"]).datetime
            doc.schema = x['schema']
            doc.rule_num += 1
        for i in rsts.values():
            for j in i.values():
                yield j


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


class StatsCMDBSQLPlan(BaseStatisticsDoc):
    """以库为单位统计sql plan"""
    sql_id = StringField(null=False)
    plan_hash_value = IntField(null=False)
    first_appearance = DateTimeField()
    last_appearance = DateTimeField()

    meta = {
        "collection": "stats_cmdb_sql_plan",
        "indexes": [
            ("sql_id", "plan_hash_value")
        ]
    }

    @classmethod
    def generate(cls, task_record_id: int, cmdb_id: Union[int, None]):
        from utils.datetime_utils import arrow
        from models.mongo import MSQLPlan
        to_aggregate = [
            {
                "$match": {
                    'cmdb_id': cmdb_id,
                    'ETL_DATE': {"$gte": arrow.now().shift(months=-3).datetime}
                }
            },
            {
                "$group": {
                    "_id": {
                        "sql_id": "$SQL_ID",
                        "plan_hash_value": "$PLAN_HASH_VALUE"
                    },
                    "first_appearance": {"$min": "$ETL_DATE"},
                    "last_appearance": {"$max": "$ETL_DATE"},
                }
            }
        ]
        ret = MSQLPlan.objects.aggregate(*to_aggregate)
        for one in ret:
            yield cls(
                task_record_id=task_record_id,
                cmdb_id=cmdb_id,
                **{k.lower(): v for k, v in one.pop("_id").items()},
                **one
            )


class StatsCMDBSQLText(BaseStatisticsDoc):
    """以库为单位统计sql text"""
    sql_id = StringField(null=False)
    first_appearance = DateTimeField()
    last_appearance = DateTimeField()
    count = IntField(default=0)

    meta = {
        "collection": "stats_cmdb_sql_text",
        "indexes": [
            "sql_id"
        ]
    }

    @classmethod
    def generate(cls, task_record_id: int, cmdb_id: Union[int, None]):
        from utils.datetime_utils import arrow
        from models.mongo import SQLText
        to_aggregate = [
            {
                "$match": {
                    'cmdb_id': cmdb_id,
                    'ETL_DATE': {"$gte": arrow.now().shift(months=-3).datetime}
                }
            },
            {
                "$group": {
                    "_id": "$SQL_ID",
                    "first_appearance": {"$min": "$ETL_DATE"},
                    "last_appearance": {"$max": "$ETL_DATE"},
                    "count": {"$sum": 1}
                }
            }
        ]
        ret = SQLText.objects.aggregate(*to_aggregate)
        for one in ret:
            yield cls(
                task_record_id=task_record_id,
                cmdb_id=cmdb_id,
                sql_id=one.pop("_id"),
                **one
            )
