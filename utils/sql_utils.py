# Author: kk.Fang(fkfkbill@gmail.com)

import re
from typing import Union

import sqlparse
from mongoengine import Q

from utils.perf_utils import *
from models.mongo import *
from models.oracle import *
from utils.datetime_utils import *
from utils import rule_utils, const
from utils.datetime_utils import *
from past.utils.constant import SQLPLUS_SQL


def parse_sql_file(sql_contents, sql_keyword):
    """读取sql文件"""

    def get_procedures_end_with_slash(sql_contents):
        cate = ["declare",
                "create\s+(?:or\s+replace\s+)?(?:EDITIONABLE|NONEDITIONABLE\s+)?(?:FUNCTION|PACKAGE|PACKAGE BODY|PROCEDURE|TRIGGER|TYPE BODY)"]
        re_str = f"\n(\s*set\s+\w+\s+\w+)|\n\s*((?:{'|'.join(cate)})[\s\S]*?end;[\s\S]*?\/)|\n\s*((?:{'|'.join(SQLPLUS_SQL)})(?:\s+.*?)?)\n|\n\s*(@@?.*?)\n"
        procedures = [''.join(x) for x in re.findall(re_str, sql_contents, re.I)]
        return procedures

    def is_annotation(sql):
        multi_annotation = re.findall("\/\*[\s\S]*?\*\/", sql, re.I)
        for anno in multi_annotation:
            sql = sql.replace(anno, "")
        return all([annotation_condition(x) for x in sql.split("\n")])

    def annotation_condition(sql):
        sql = sql.strip()
        if not sql:
            return True
        if re.match("^\s*(remark|rem|--|\/\*)\s+", sql, re.I):
            return True
        if re.match('\s*--.*?', sql, re.I):
            return True
        if re.match('\/\* +.*?\*\/', sql, re.I):
            return True
        if re.match('\/\*[^+]{2}[\s\S]*?\*\/\n', sql, re.I):
            return True
        return False


    # sql_keyword doesn't used.

    procedures = get_procedures_end_with_slash(sql_contents)
    for procedure in procedures:
        sql_contents = sql_contents.replace(procedure, "|||||")

    sql_contents = [x.strip(' ') for x in sql_contents.split("|||||")]

    sql_list = []
    for index, content in enumerate(sql_contents):
        sql_list += [a for b in [sqlparse.split(x) for x in content.split(';')] for a in b]
        if index < len(procedures) and procedures[index].strip():
            sql_list.append(procedures[index].strip())

    sql_list = [sql for sql in sql_list if sql]

    new_sql_list = []
    annotation_sql = ""
    for sql in sql_list:

        if is_annotation(sql):
            annotation_sql += sql
        else:
            # new_sql_list.append(
            #     (annotation_sql + "\n" + sql).lstrip().replace('\n\n', '\n').replace('\n', '<br/>').replace("\"", "'"))
            new_sql_list.append(
                (annotation_sql + "\n" + sql).lstrip())
            annotation_sql = ""

    return new_sql_list


@timing(cache=r_cache)
def get_sql_id_stats(cmdb_id, etl_date_gte=None) -> dict:
    """
    计算sql文本的统计信息
    :param cmdb_id:
    :param etl_date_gte: etl时间晚于
    :return: {sql_id: {}}
    """
    # TODO use cache!
    # TODO use bulk aggregation instead of aggregate one by one!

    match_case = {
        'cmdb_id': cmdb_id,
        # 'etl_date': {"$gte": , "$lt": }
    }
    if etl_date_gte:
        match_case["etl_date"] = {}
        match_case["etl_date"]["$gte"] = etl_date_gte
    to_aggregate = [
        {
            "$match": match_case
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
    return {i["_id"]: i for i in ret}


def __prefetch():
    with make_session() as session:
        for cmdb in session.query(CMDB).all():
            get_sql_id_stats(cmdb_id=cmdb.cmdb_id)


get_sql_id_stats.prefetch = __prefetch
del __prefetch


@timing(cache=r_cache)
def get_sql_plan_stats(cmdb_id, etl_date_gte=None) -> dict:
    """
    计算sql计划的统计信息
    :param cmdb_id:
    :param etl_date_gte: etl时间晚于
    :return: {plan_hash_value: {}}
    """
    # TODO use cache!
    # TODO use bulk aggregation instead of aggregate one by one!
    if not etl_date_gte:
        etl_date_gte = arrow.now().shift(months=-1).date()
    match_case = {
        'cmdb_id': cmdb_id,
        'etl_date': {"$gte": etl_date_gte}
    }
    to_aggregate = [
        {
            "$match": match_case
        },
        {
            "$group": {
                "_id": "$PLAN_HASH_VALUE",
                "first_appearance": {"$min": "$ETL_DATE"},
                "last_appearance": {"$max": "$ETL_DATE"},
            }
        }
    ]
    ret = MSQLPlan.objects.aggregate(*to_aggregate)
    return {i["_id"]: i for i in ret}


def __prefetch():
    with make_session() as session:
        for cmdb in session.query(CMDB):
            get_sql_plan_stats(cmdb_id=cmdb.cmdb_id)


get_sql_plan_stats.prefetch = __prefetch
del __prefetch


@timing(cache=r_cache)
def get_sql_id_sqlstat_dict(record_id: Union[tuple, list, str]) -> dict:
    """
    获取最近捕获的sql文本统计信息(在给定的record_id中)
    :param record_id: 可传单个或者list
    :return: {sql_id: {stats, ...}, ...}
    """
    if not isinstance(record_id, (list, tuple)):
        if isinstance(record_id, str):
            record_id = [record_id]
        else:
            assert 0
    keys = ["sql_id", "elapsed_time_delta", "executions_delta", "schema"]
    return {i[0]: dict(zip(keys[1:], i[1:])) for i in
            SQLStat.objects(record_id__in=record_id).order_by("-etl_date").values_list(*keys)}


@timing(cache=r_cache)
def get_risk_sql_list(session,
                      cmdb_id: int,
                      date_range: (date, date),
                      schema_name: str = None,
                      rule_type: str = "ALL",
                      risk_sql_rule_id: list = (),
                      sort_by: str = "sum",
                      enable_white_list: bool = True,
                      sql_id_only: bool = False,
                      sqltext_stats: bool = True,
                      severity: Union[tuple, list, None] = None,
                      task_record_id: int = None,
                      **kwargs
                      ) -> Union[dict, set]:
    """
    获取风险SQL列表
    :param session:
    :param cmdb_id:
    :param date_range:
    :param schema_name:
    :param rule_type:
    :param risk_sql_rule_id:
    :param sort_by:
    :param enable_white_list:
    :param sql_id_only: 仅仅返回sql_id的set
    :param sqltext_stats: 返回是否需要包含sqltext的统计信息（首末出现时间）
    :param severity: 严重程度过滤
    :param task_record_id: 仅展示task_record_id指定的results, 如果指定了，则忽略开始结束时间
    :param kwargs: 多余的参数，会被收集到这里，并且会提示
    :return:
    """
    # 因为参数过多，加个判断。
    date_start, date_end = date_range
    assert isinstance(date_start, date) and not isinstance(date_start, datetime)
    assert isinstance(date_end, date) and not isinstance(date_end, datetime)
    assert sort_by in ("sum", "average")
    assert rule_type in ["ALL"] + const.ALL_RULE_TYPES_FOR_SQL_RULE
    if kwargs:
        print(f"got extra useless kwargs: {kwargs}")

    cmdb = session.query(CMDB).filter_by(cmdb_id=cmdb_id).first()
    if task_record_id:
        result_q = Results.filter_by_exec_hist_id(task_record_id).filter(cmdb_id=cmdb_id)
    else:
        result_q = Results.objects(cmdb_id=cmdb_id)
    if schema_name:
        result_q = result_q.filter(schema_name=schema_name)
    if rule_type == "ALL":
        rule_type: list = const.ALL_RULE_TYPES_FOR_SQL_RULE
    else:
        rule_type: list = [rule_type]
    risk_rule_q = session.query(RiskSQLRule).\
        filter(RiskSQLRule.rule_type.in_(rule_type),
               RiskSQLRule.db_model == cmdb.db_model)
    result_q = result_q.filter(rule_type__in=rule_type)

    if risk_sql_rule_id:
        risk_rule_q = risk_rule_q.filter(RiskSQLRule.risk_sql_rule_id.
                                         in_(risk_sql_rule_id))
    if severity:
        risk_rule_q = risk_rule_q.filter(RiskSQLRule.severity.in_(severity))
    if date_start and not task_record_id:
        result_q = result_q.filter(create_date__gte=date_start)
    if date_end and not task_record_id:
        result_q = result_q.filter(create_date__lte=date_end)
    risky_rules = Rule.filter_enabled(
        rule_name__in=[i[0] for i in risk_rule_q.with_entities(RiskSQLRule.rule_name)],
        db_model=cmdb.db_model,
    )
    risk_rules_dict = rule_utils.get_risk_rules_dict(session)
    risky_rule_name_object_dict = {risky_rule.rule_name:
                                       risky_rule for risky_rule in risky_rules.all()}

    get_risk_sql_list.tik(f"risk sql rule count: {len(risky_rule_name_object_dict)}")

    # 过滤出包含风险SQL规则结果的result
    Qs = None
    for risky_rule_name in risky_rule_name_object_dict.keys():
        if not Qs:
            Qs = Q(**{f"{risky_rule_name}__sqls__nin": [None, []]})
        else:
            Qs = Qs | Q(**{f"{risky_rule_name}__sqls__nin": [None, []]})
    if Qs:
        result_q = result_q.filter(Qs)
    get_risk_sql_list.tik(f"result count: {result_q.count()}")

    rst = []  # 详细信息的返回结果
    rst_sql_id_set = set()  # 统计sql_id防止重复

    if not sql_id_only:
        # ====== 如果仅统计sql_id，以下信息不需要 ======
        sql_text_stats = {}
        if sqltext_stats:
            sql_text_stats = get_sql_id_stats(cmdb_id=cmdb_id)
        # 统计全部搜索到的result的record_id内的全部sql_id的最近一次运行的统计信息
        last_sql_id_sqlstat_dict = get_sql_id_sqlstat_dict(record_id=list(result_q.distinct("record_id")))

    if enable_white_list:
        white_list_dict = rule_utils.get_white_list(session, cmdb_id)

    for result in result_q:

        # result具有可变字段，具体结构请参阅models.mongo.results

        for risky_rule_name, risky_rule_object in risky_rule_name_object_dict.items():
            risk_rule_object = risk_rules_dict[risky_rule_object.get_3_key()]

            # risky_rule_object is a record of Rule from mongodb

            # risk_rule_object is a record of RiskSQLRule from oracle

            if not getattr(result, risky_rule_name, None):
                continue  # 规则key不存在，或者值直接是个空dict，则跳过
            if not getattr(result, risky_rule_name).get("sqls", None):
                # 规则key下的sqls不存在，或者值直接是个空list，则跳过
                # e.g. {"XXX_RULE_NAME": {"scores": 0.0}}  # 无sqls
                # e.g. {"XXX_RULE_NAME": {"sqls": [], "scores": 0.0}}
                continue

            sqls = getattr(result, risky_rule_name)["sqls"]

            if sql_id_only:
                rst_sql_id_set.update([i["sql_id"] for i in sqls])
                continue

            # NOTICE: 以下代码必须保证sql_id_only == False

            for sql_text_dict in sqls:
                sql_id = sql_text_dict["sql_id"]
                if sql_id in rst_sql_id_set:
                    continue

                sqlstat_dict = last_sql_id_sqlstat_dict[sql_id]

                if enable_white_list:
                    for wl_dict in white_list_dict:
                        if wl_dict["rule_category"] == const.WHITE_LIST_CATEGORY_TEXT:
                            if wl_dict["rule_text"] in sql_text_dict["sql_text"]:
                                continue
                        elif wl_dict["rule_category"] == const.WHITE_LIST_CATEGORY_USER:
                            if wl_dict["rule_text"] == sqlstat_dict["schema"]:
                                continue

                execution_time_cost_sum = round(sqlstat_dict["elapsed_time_delta"], 2)  # in ms
                execution_times = sqlstat_dict.get('executions_delta', 0)
                execution_time_cost_on_average = 0
                if execution_times:
                    execution_time_cost_on_average = round(execution_time_cost_sum / execution_times, 2)
                r = {
                    "sql_id": sql_id,
                    "schema": sqlstat_dict["schema"],
                    "sql_text": sql_text_dict["sql_text"],
                    "rule_desc": risky_rule_object.rule_desc,
                    "severity": risk_rule_object.severity,
                    "similar_sql_num": 1,  # sql_text_stats[sql_id]["count"],  # TODO 这是啥？
                    "execution_time_cost_sum": execution_time_cost_sum,
                    "execution_times": execution_times,
                    "execution_time_cost_on_average": execution_time_cost_on_average,
                    "risk_sql_rule_id": risk_rule_object.risk_sql_rule_id,
                }
                if sqltext_stats:
                    sql_text_stats_sql_id = sql_text_stats.get(sql_id, {})
                    r.update({
                        "first_appearance": dt_to_str(sql_text_stats_sql_id.get('first_appearance', None)),
                        "last_appearance": dt_to_str(sql_text_stats_sql_id.get('last_appearance', None)),
                    })
                rst.append(r)
                rst_sql_id_set.add(sql_id)
    if sql_id_only:
        return rst_sql_id_set
    if sort_by == "sum":
        rst = sorted(rst, key=lambda x: x["execution_time_cost_sum"], reverse=True)
    elif sort_by == "average":
        rst = sorted(rst, key=lambda x: x["execution_time_cost_on_average"], reverse=True)
    return rst
