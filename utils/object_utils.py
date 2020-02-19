# Author: kk.Fang(fkfkbill@gmail.com)

from typing import Iterable, Union

from mongoengine import Q
from sqlalchemy import func

from models.oracle import *
from models.mongo import *
from utils.perf_utils import *
from utils.datetime_utils import *
from utils import rule_utils, const, cmdb_utils
from utils.conc_utils import *


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
                         rule_name: Union[None, str, list, tuple] = None,
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
    :param rule_name:
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
    if rule_name and isinstance(rule_name, str):
        risk_rule_q = risk_rule_q.filter(RiskSQLRule.rule_name == rule_name)
    if rule_name and isinstance(rule_name, (list, tuple)):
        risk_rule_q = risk_rule_q.filter(RiskSQLRule.rule_name.in_(rule_name))
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
                    "schema": result.schema_name,
                    "object_name": record[0],
                    "rule_desc": risky_rule_object.rule_desc,
                    "table_name": risky_rule_object.get_object_name(
                        record, const.OBJ_RULE_TYPE_TABLE),
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
                r.update({
                    "rule": risky_rule_object.to_dict(iter_if=lambda k, v: k in (
                        "rule_name", "rule_desc", "obj_info_type")),
                })
                rst.append(r)

    return rst


def risk_object_export_data(cmdb_id=None, schema=None,
                                  date_start=None, date_end=None,
                                  severity: list = None, rule_name: list = None,
                                  ids: list = None):
    """风险对象导出数据获取"""
    risk_objects = StatsRiskObjectsRule.objects(cmdb_id=cmdb_id)
    if schema:
        risk_objects = risk_objects.filter(schema=schema)
    if date_start:
        risk_objects = risk_objects.filter(etl_date__gte=date_start)
    if date_end:
        risk_objects = risk_objects.filter(etl_date__lte=date_end)
    if severity:
        risk_objects = risk_objects.filter(severity__in=severity)
    if rule_name:
        risk_objects = risk_objects.filter(rule__rule_name__in=rule_name)
    if ids:
        risk_objects = risk_objects.filter(_id__in=ids)
        # 如果指定了统计表的id，则只需要这些id的rule_name作为需要导出的数据
        rule_name: list = [a_rule["rule_name"]
                           for a_rule in risk_objects.values_list("rule")]
    rr = []
    for x in risk_objects:
        d = x.to_dict()
        d.update({**d.pop("rule")})
        rr.append(d)

    with make_session() as session:
        rst = get_risk_object_list(
            session=session,
            cmdb_id=cmdb_id,
            schema_name=schema,
            date_end=date_end,
            date_start=date_start,
            severity=severity,
            rule_name=rule_name
        )

    return rr, rst


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
