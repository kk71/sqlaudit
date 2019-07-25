# Author: kk.Fang(fkfkbill@gmail.com)

from typing import Iterable, Union

from mongoengine import Q
from sqlalchemy import func

from models.oracle import *
from models.mongo import *
from utils.perf_utils import *
from utils.datetime_utils import *
from utils import rule_utils, const, cmdb_utils


@timing(cache=r_cache)
def get_cmdb_phy_size(session, cmdb_id) -> int:
    """
    计算cmdb最近一次统计的物理体积（目前仅计算表的总体积）
    :param session:
    :param cmdb_id:
    :return: int
    """
    cmdb = session.query(CMDB).filter_by(cmdb_id=cmdb_id).first()
    latest_task_exec_hist_obj = session.query(TaskExecHistory). \
        filter(TaskExecHistory.connect_name == cmdb.connect_name). \
        order_by(TaskExecHistory.task_end_date.desc()).first()
    if latest_task_exec_hist_obj:
        ret = list(ObjTabInfo.objects.aggregate(
            {
                "$match": {
                    "record_id": {"$regex": f"{latest_task_exec_hist_obj.id}"},
                    "cmdb_id": cmdb.cmdb_id
                }
            },
            {
                "$group": {
                    "_id": "$cmdb_id",
                    "sum": {"$sum": "$PHY_SIZE(MB)"}
                }
            }
        ))
        if ret:
            return ret[0]["sum"]


def __prefetch():
    with make_session() as session:
        for cmdb in session.query(CMDB):
            get_cmdb_phy_size(session=session, cmdb_id=cmdb.cmdb_id)


get_cmdb_phy_size.prefetch = __prefetch
del __prefetch


@timing(cache=r_cache)
def get_object_stats_towards_cmdb(rule_names: Iterable, cmdb_id: int) -> dict:
    """
    统计某个库的obj的规则的首末次出现时间
    :param rule_names:
    :param cmdb_id:
    :return:
    """
    ret = {}
    for rule_name in rule_names:
        to_aggregate = [
            {
                "$match": {
                    "$and": [
                        {rule_name + ".records": {"$exists": True}},
                        {rule_name + ".records": {"$not": {"$size": 0}}},
                        {"cmdb_id": cmdb_id}
                    ]
                }
            },
            {
                "$group": {
                    '_id': rule_name,
                    "first_appearance": {"$min": "$create_date"},
                    "last_appearance": {"$max": "$create_date"}
                }
            },
            {
                "$project": {
                    '_id': 0,
                    'first_appearance': 1,
                    'last_appearance': 1
                }
            }
        ]
        r = list(Results.objects.aggregate(*to_aggregate))
        if r:
            ret[rule_name] = {
                k: dt_to_str(v)
                for k, v in r[0].items()}
    return ret


def __prefetch():
    with make_session() as session:
        for cmdb in session.query(CMDB).all():
            rule_names = list(Rule.objects.filter(db_model=cmdb.db_model).
                              values_list("rule_name"))
            get_object_stats_towards_cmdb(rule_names=rule_names, cmdb_id=cmdb.cmdb_id)


get_object_stats_towards_cmdb.prefetch = __prefetch
del __prefetch


@timing(cache=r_cache)
def get_risk_object_list(session,
                         cmdb_id,
                         date_start=None,
                         date_end=None,
                         schema_name=None,
                         severity: Union[None, tuple, list] = None,
                         risk_sql_rule_id: Union[tuple, list] = (),
                         task_record_id: int = None,
                         **kwargs):
    """
    获取风险对象列表
    :param session:
    :param cmdb_id:
    :param date_start:
    :param date_end:
    :param schema_name:
    :param severity: 严重程度过滤
    :param risk_sql_rule_id:
    :param task_record_id: 仅展示task_record_id指定的results, 如果指定了，则忽略开始结束时间
    :return:
    """
    risk_sql_rule_id_list = risk_sql_rule_id
    if kwargs:
        print(f"got extra useless kwargs: {kwargs}")

    cmdb = session.query(CMDB).filter_by(cmdb_id=cmdb_id).first()
    if task_record_id:
        result_q = Results.filter_by_exec_hist_id(task_record_id).filter(
            cmdb_id=cmdb_id,
            rule_type=rule_utils.RULE_TYPE_OBJ
        )
    else:
        result_q = Results.objects(
            cmdb_id=cmdb_id,
            rule_type=rule_utils.RULE_TYPE_OBJ
        )
    if schema_name:
        result_q = result_q.filter(schema_name=schema_name)
    risk_rule_q = session.query(RiskSQLRule). \
        filter(RiskSQLRule.rule_type == rule_utils.RULE_TYPE_OBJ,
               RiskSQLRule.db_model == cmdb.db_model)
    if risk_sql_rule_id_list:
        risk_rule_q = risk_rule_q.filter(RiskSQLRule.risk_sql_rule_id.
                                         in_(risk_sql_rule_id_list))
    if severity:
        risk_rule_q = risk_rule_q.filter(RiskSQLRule.severity.in_(severity))
    if date_start and not task_record_id:
        result_q = result_q.filter(create_date__gte=date_start)
    if date_end and not task_record_id:
        result_q = result_q.filter(create_date__lte=date_end)
    risky_rules = Rule.filter_enabled(
        rule_name__in=[i[0] for i in risk_rule_q.with_entities(RiskSQLRule.rule_name)],
        db_model=cmdb.db_model
    )
    risk_rules_dict = rule_utils.get_risk_rules_dict(session)
    risky_rule_name_object_dict = {risky_rule.rule_name:
                                       risky_rule for risky_rule in risky_rules}
    # if not risky_rule_name_object_dict:
    #     raise const.NoRiskRuleSetException

    # 过滤出包含问题的结果
    Qs = None
    for risky_rule_name in risky_rule_name_object_dict.keys():
        if not Qs:
            Qs = Q(**{f"{risky_rule_name}__records__nin": [None, []]})
        else:
            Qs = Qs | Q(**{f"{risky_rule_name}__records__nin": [None, []]})
    if Qs:
        result_q = result_q.filter(Qs)

    risky_rule_appearance = get_object_stats_towards_cmdb(
        rule_names=risky_rule_name_object_dict.keys(),
        cmdb_id=cmdb_id
    )
    rst = []
    rst_set_for_deduplicate = set()  # 集合内为tuples，tuple内的值是返回字典内的values（按顺序）
    for result in result_q:
        for risky_rule_name, risky_rule_object in risky_rule_name_object_dict.items():
            risk_rule_object = risk_rules_dict.get(risky_rule_object.get_3_key(), None)
            if not risk_rule_object:
                continue

            # risky_rule_object is a record of Rule from mongodb

            # risk_rule_object is a record of RiskSQLRule from oracle

            if not getattr(result, risky_rule_name, None):
                continue  # 规则key不存在，或者值直接是个空dict
            if not getattr(result, risky_rule_name).get("records", None):
                continue  # 规则key存在，值非空，但是其下records的值为空
            for record in getattr(result, risky_rule_name)["records"]:
                r = {
                    "object_name": record[0],
                    "rule_desc": risky_rule_object.rule_desc,
                    "risk_detail": rule_utils.format_rule_result_detail(
                        risky_rule_object, record),
                    "optimized_advice": risk_rule_object.optimized_advice,
                    "severity": risk_rule_object.severity,
                    "risk_sql_rule_id": risk_rule_object.risk_sql_rule_id,
                    **risky_rule_appearance[risky_rule_name]
                }
                # 用于去重
                r_tuple = tuple(r.values())
                if r_tuple in rst_set_for_deduplicate:
                    continue
                rst_set_for_deduplicate.add(r_tuple)
                rst.append(r)

    return rst


def __prefetch():
    arrow_now = arrow.now()
    date_end = arrow_now.shift(days=+1).date()
    date_start_week = arrow_now.shift(weeks=-1).date()
    date_start_month = arrow_now.shift(days=-30).date()
    with make_session() as session:
        for cmdb in session.query(CMDB).all():
            get_risk_object_list(
                session=session,
                cmdb_id=cmdb.cmdb_id,
                date_start=date_start_week,
                date_end=date_end,
                schema_name=None,
                risk_sql_rule_id=None
            )
            get_risk_object_list(
                session=session,
                cmdb_id=cmdb.cmdb_id,
                date_start=date_start_month,
                date_end=date_end,
                schema_name=None,
                risk_sql_rule_id=None
            )


get_risk_object_list.prefetch = __prefetch
del __prefetch


@timing(cache=r_cache)
def dashboard_3_sum(session, login_user):
    """
    仪表盘的三个数字 以及可用的exec hist id列表
    :param session:
    :param login_user:
    :return:
    """
    cmdb_ids = cmdb_utils.get_current_cmdb(session, login_user)
    # 获取每个库最后一次抓取分析成功的历史记录的id
    sub_q = session. \
        query(TaskExecHistory.id.label("id"), TaskManage.cmdb_id.label("cmdb_id")). \
        join(TaskExecHistory, TaskExecHistory.connect_name == TaskManage.connect_name). \
        filter(TaskManage.cmdb_id.in_(cmdb_ids),
               TaskManage.task_exec_scripts == const.DB_TASK_CAPTURE,
               TaskExecHistory.status == True).subquery()
    cmdb_id_exec_hist_id_list_q = session. \
        query(sub_q.c.cmdb_id, func.max(sub_q.c.id)).group_by(sub_q.c.cmdb_id)
    task_exec_hist_id_list: [str] = [str(i[1]) for i in cmdb_id_exec_hist_id_list_q]

    sql_num = len(SQLText.filter_by_exec_hist_id(task_exec_hist_id_list).distinct("sql_id"))
    table_num = ObjTabInfo.filter_by_exec_hist_id(task_exec_hist_id_list).count()
    index_num = ObjIndColInfo.filter_by_exec_hist_id(task_exec_hist_id_list).count()

    return sql_num, table_num, index_num, task_exec_hist_id_list


def __prefetch():
    with make_session() as session:
        users = session.query(User.login_user).all()
        for user in users:
            dashboard_3_sum(session=session, login_user=user[0])


dashboard_3_sum.prefetch = __prefetch
del __prefetch
