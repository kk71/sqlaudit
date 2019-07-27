# Author: kk.Fang(fkfkbill@gmail.com)

from collections import defaultdict
from typing import Union

from sqlalchemy import func
from mongoengine import Q

from utils.const import *
from utils.perf_utils import timing
from models.mongo import *
from models.oracle import *
from utils import rule_utils


def calc_deduction(scores):
    """扣分计算"""
    return round(float(scores) / 1.0, 2)


def calc_weighted_deduction(scores, max_score_sum):
    """加权扣分计算"""
    return round(float(scores) * 100 / (max_score_sum or 1), 2)


def calc_result(result, db_model) -> tuple:
    """
    计算单个result的分数
    :param result: mongodb result object
    :param db_model:
    :return:
    """
    score_sum_of_all_rule_scores_in_result = 0
    max_score_sum = rule_utils.calc_sum_of_rule_max_score(
        db_type=DB_ORACLE,
        db_model=db_model,
        rule_type=result.rule_type
    )
    rule_name_to_detail = defaultdict(lambda: {
        "violated_num": 0,
        "rule": {},
        "deduction": 0.0,
        "weighted_deduction": 0.0
    })

    for rule_object in Rule.objects(rule_type=result.rule_type,
                                    db_model=db_model,
                                    db_type=DB_ORACLE):
        rule_result = getattr(result, rule_object.rule_name, None)
        if rule_result and (rule_result.get("sqls", []) or rule_result.get("records", [])):
            if result.rule_type == RULE_TYPE_OBJ:
                rule_name_to_detail[rule_object.rule_name]["violated_num"] += \
                    len(rule_result.get("records", []))

            elif result.rule_type in ALL_RULE_TYPES_FOR_SQL_RULE:
                rule_name_to_detail[rule_object.rule_name]["violated_num"] += \
                    len(rule_result.get("sqls", []))

            else:
                assert 0

            score_sum_of_all_rule_scores_in_result += \
                round(float(rule_result["scores"]) / 1.0, 2)
            rule_name_to_detail[rule_object.rule_name]["rule"] = rule_object.to_dict(
                iter_if=lambda key, v: key in ("rule_name", "rule_desc"))
            rule_name_to_detail[rule_object.rule_name]["deduction"] += \
                calc_deduction(rule_result["scores"])
            rule_name_to_detail[rule_object.rule_name]["weighted_deduction"] += \
                calc_weighted_deduction(
                    rule_result["scores"],
                    max_score_sum=max_score_sum
                )
    scores_total = round((max_score_sum - score_sum_of_all_rule_scores_in_result) /
                         max_score_sum * 100 or 1, 2)
    scores_total = scores_total if scores_total > 40 else 40

    return list(rule_name_to_detail.values()), scores_total


@timing()
def calc_score_by(session, cmdb, perspective, score_by) -> dict:
    """
    获取某个纳管数据库最后一次的按照[规则类型/schema]分类的评分
    :param session:
    :param cmdb:
    :param perspective:
    :param score_by:
    :return:
    """
    # TODO make it cached
    assert perspective in ALL_OVERVIEW_ITEM
    assert score_by in ALL_SCORE_BY

    ret = {}

    last_exec_hist = TaskExecHistory.\
        filter_succeed(session, TaskExecHistory.connect_name == cmdb.connect_name).\
        order_by(TaskExecHistory.task_end_date.desc()).first()
    if not last_exec_hist:
        calc_score_by.tik("no last exec hist")
        return ret  # 无分析记录
    calc_score_by.tik(f"last exec hist id {last_exec_hist.id}")

    rule_type_schema_scores = Job.filter_by_exec_hist_id(last_exec_hist.id). \
        values_list("desc__rule_type", "desc__owner", "score")
    scores_by_sth = defaultdict(lambda: defaultdict(lambda: 0.0))
    for rule_type, schema, score in rule_type_schema_scores:
        if score:
            if perspective == OVERVIEW_ITEM_RADAR:
                scores_by_sth[rule_type][schema] += score
            elif perspective == OVERVIEW_ITEM_SCHEMA:
                scores_by_sth[schema][rule_type] += score
    calc_score_by.tik("end calcing scores")

    for persp_1, persp_2_score_dict in scores_by_sth.items():
        ret[persp_1] = None
        if score_by == SCORE_BY_LOWEST:
            scores_sorted = [s for s in
                             sorted(persp_2_score_dict.items(), key=lambda k: k[1]) if s]
            if scores_sorted:
                ret[persp_1] = scores_sorted[0][1]

        elif score_by == SCORE_BY_AVERAGE:
            persp_2_num = len(persp_2_score_dict)
            if persp_2_num:
                ret[persp_1] = sum(persp_2_score_dict.values()) / persp_2_num

    return ret


def get_latest_task_record_id(session, cmdb_id: Union[list, int]) -> dict:
    """
    获取每个库最后一次采集分析的task_record_id
    :param session:
    :param cmdb_id:
    :return: {cmdb_id: task_record_id, ...}
    """
    if not isinstance(cmdb_id, (tuple, list)):
        cmdb_id = [cmdb_id]
    sub_q = session. \
        query(TaskExecHistory.id.label("id"), TaskManage.cmdb_id.label("cmdb_id")). \
        join(TaskExecHistory, TaskExecHistory.connect_name == TaskManage.connect_name). \
        filter(TaskManage.cmdb_id.in_(cmdb_id),
               TaskManage.task_exec_scripts == DB_TASK_CAPTURE,
               TaskExecHistory.status == True).subquery()  # 必须取成功的执行记录
    cmdb_id_exec_hist_id_list_q = session. \
        query(sub_q.c.cmdb_id, func.max(sub_q.c.id)).group_by(sub_q.c.cmdb_id)
    return dict(list(cmdb_id_exec_hist_id_list_q))


def get_result_queryset_by_type(
        task_record_id,
        rule_type: Union[str, list, tuple],
        obj_info_type=None):
    """
    复杂查询results
    :param task_record_id:
    :param rule_type:
    :param obj_info_type: 仅适用于当rule_type为OBJ的时候
    :return: (results_queryset, rule_names_to_filter)
    """
    if rule_type != RULE_TYPE_OBJ and obj_info_type:
        assert 0
    if isinstance(rule_type, str):
        rule_type = [rule_type]
    rule_names_to_filter = []
    if obj_info_type:
        # 默认规则只过滤已经启用的
        rule_names_to_filter = Rule.\
            filter_enabled(rule_type=RULE_TYPE_OBJ, obj_info_type=obj_info_type).\
            values_list("rule_name")
    result_q = Results.filter_by_exec_hist_id(task_record_id).filter(rule_type__in=rule_type)
    if rule_names_to_filter and obj_info_type:
        Qs = None
        for rn in rule_names_to_filter:
            if not Qs:
                Qs = Q(**{f"{rn}__sqls__nin": [None, []]}) |\
                     Q(**{f"{rn}__records__nin": [None, []]})
            else:
                Qs = Qs |\
                     Q(**{f"{rn}__sqls__nin": [None, []]}) |\
                     Q(**{f"{rn}__records__nin": [None, []]})
        if Qs:
            result_q = result_q.filter(Qs)
    return result_q, rule_names_to_filter


def calc_distinct_sql_id(result_q, rule_name: Union[str, list, tuple] = None) -> int:
    """
    计算result的query set的sql_id去重
    目前只计算非OBJ类型的
    :param result_q:
    :param rule_name: 指定规则名称，支持单个或者列表，默认不传则表示统计全部
    :return:
    """
    sql_id_set = set()
    if isinstance(rule_name, str):
        rule_name = [rule_name]
    for result in result_q:
        if not rule_name:
            rule_name_to_loop = dir(result)
        else:
            rule_name_to_loop = rule_name
        for rn in rule_name_to_loop:
            result_rule_dict = getattr(result, rn, None)
            if not result_rule_dict or not isinstance(result_rule_dict, dict):
                continue
            sqls = result_rule_dict.get("sqls", [])
            if not sqls:
                continue
            for sql in sqls:
                sql_id_set.add(sql["sql_id"])
    return len(sql_id_set)


def calc_problem_num(result_q, rule_name: Union[str, list, tuple] = None) -> int:
    """
    计算result的query set的问题发生次数
    目前只计算OBJ类型的
    :param result_q:
    :param rule_name: 指定规则名称，支持单个或者列表，默认不传则表示统计全部
    :return:
    """
    count = 0
    if isinstance(rule_name, str):
        rule_name = [rule_name]
    for result in result_q:
        if not rule_name:
            rule_name_to_loop = dir(result)
        else:
            rule_name_to_loop = rule_name
        for rn in rule_name_to_loop:
            result_rule_dict = getattr(result, rn, None)
            if not result_rule_dict or not isinstance(result_rule_dict, dict):
                continue
            records = result_rule_dict.get("records", [])
            if not records:
                continue
            count += len(records)
    return count
