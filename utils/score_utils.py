# Author: kk.Fang(fkfkbill@gmail.com)

from typing import Union
from collections import defaultdict

from sqlalchemy import func
from mongoengine import Q

from utils.const import *
from utils.perf_utils import timing
from models.mongoengine import *
from models.sqlalchemy import *
from utils import rule_utils
from utils.datetime_utils import *


def calc_deduction(scores):
    """扣分计算"""
    return round(float(scores) / 1.0, 2)


def calc_weighted_deduction(scores, max_score_sum):
    """加权扣分计算"""
    return round(float(scores) * 100 / (max_score_sum or 1), 2)


def calc_result(result, db_model, obj_info_type: Union[str, list, tuple] = None) -> tuple:
    """
    计算单个result的分数
    :param result: mongodb result object
    :param db_model:
    :param obj_info_type: 只展示某些obj_info_type的类型
    :return:
    """
    if isinstance(obj_info_type, str):
        obj_info_type = [obj_info_type]

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
    rule_q = Rule.objects(
        rule_type=result.rule_type,
        db_model=db_model,
        db_type=DB_ORACLE
    )
    if obj_info_type:
        rule_q = rule_q.filter(obj_info_type__in=obj_info_type)

    for rule_object in rule_q:
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
    if max_score_sum == 0:
        print(f"total score is 0!!! {result}")
        scores_total = 0
    else:
        scores_total = round((max_score_sum - score_sum_of_all_rule_scores_in_result) /
                             max_score_sum * 100 or 1, 2)
    scores_total = scores_total if scores_total > 40 else 40

    return list(rule_name_to_detail.values()), scores_total


@timing()
def calc_score_by(session, cmdb, perspective, score_by) -> dict:
    """
    获取某个纳管数据库最后一次的按照[规则类型/schema]分类的评分
    评分仅包括配置过评分的schema
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

    latest_task_record_id = get_latest_task_record_id(session, cmdb_id=cmdb.cmdb_id).\
        get(cmdb.cmdb_id, None)
    if not latest_task_record_id:
        calc_score_by.tik("No latest task_record_id was found, "
                          "or this task has never run.")
        return ret  # 无分析记录

    q = StatsSchemaRate.objects(
        task_record_id=latest_task_record_id,
        add_to_rate=True
    )
    scores_by_sth = defaultdict(lambda: defaultdict(lambda: 0.0))
    for schema_rate in q:
        schema = schema_rate.schema_name
        for rule_type, rule_type_dict in schema_rate.score_rule_type.items():
            score = rule_type_dict["score"]
            if score:
                if perspective == OVERVIEW_ITEM_RADAR:
                    scores_by_sth[rule_type][schema] += score
                elif perspective == OVERVIEW_ITEM_SCHEMA:
                    scores_by_sth[schema][rule_type] += score

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


def get_latest_task_record_id(
        session,
        cmdb_id: Union[list, int, None] = None,
        status: Union[bool, None] = True,
        task_start_date_gt: Union[datetime, callable, None] =
        lambda: arrow.now().shift(days=-7).datetime,
        task_record_id_to_replace: [dict, None] = None
) -> dict:
    """
    获取每个库最后一次采集分析的task_record_id
    :param session:
    :param cmdb_id:
    :param status: 是否指定结束状态？True表示只过滤成功的，False表示失败，None表示不过滤
    :param task_start_date_gt: 搜索的task_record必须晚于某个时间点(datetime)
    :param task_record_id_to_replace: 提供一个替换的{cmdb_id: record_id}
    :return: {cmdb_id: task_record_id, ...}
    """
    if callable(task_start_date_gt):
        task_start_date_gt: Union[datetime, None] = task_start_date_gt()
    sub_q = session. \
        query(TaskExecHistory.id.label("id"), TaskManage.cmdb_id.label("cmdb_id")). \
        join(TaskExecHistory, TaskExecHistory.connect_name == TaskManage.connect_name). \
        filter(TaskManage.task_exec_scripts == DB_TASK_CAPTURE)
    if cmdb_id:
        if not isinstance(cmdb_id, (tuple, list)):
            cmdb_id = [cmdb_id]
        sub_q = sub_q.filter(TaskManage.cmdb_id.in_(cmdb_id))
    if status is not None:
        sub_q = sub_q.filter(TaskExecHistory.status == status)
    if task_start_date_gt is not None:
        sub_q = sub_q.filter(TaskExecHistory.task_start_date > task_start_date_gt)
    sub_q = sub_q.subquery()
    cmdb_id_exec_hist_id_list_q = session. \
        query(sub_q.c.cmdb_id, func.max(sub_q.c.id)).group_by(sub_q.c.cmdb_id)
    ret = dict(list(cmdb_id_exec_hist_id_list_q))
    if task_record_id_to_replace:
        ret.update(task_record_id_to_replace)
    return ret


def get_result_queryset_by(
        task_record_id,
        rule_type: Union[str, list, tuple] = None,
        obj_info_type=None,
        schema_name: Union[str, list, tuple] = None,
        cmdb_id: Union[int, list, tuple] = None,
        cmdb_id_schema_name_pairs: [tuple] = None
):
    """
    复杂查询results
    :param task_record_id:
    :param rule_type:
    :param obj_info_type:
    :param schema_name: 过滤schema_name
    :param cmdb_id:
    :param cmdb_id_schema_name_pairs: [(981, "APEX"), ...] or [(981, ("APEX", "ABC"))]
    :return: (results_queryset, rule_names_to_filter)
    """
    if isinstance(rule_type, str):
        rule_type = [rule_type]
    if isinstance(schema_name, str):
        schema_name = [schema_name]
    if isinstance(cmdb_id, int):
        cmdb_id = [cmdb_id]
    rule_names_to_filter = []
    if obj_info_type:
        # 默认规则只过滤已经启用的
        rule_names_to_filter = list(set(Rule.filter_enabled(obj_info_type=obj_info_type).
                                        values_list("rule_name")))
    result_q = Results.filter_by_exec_hist_id(task_record_id)
    if rule_type:
        result_q = result_q.filter(rule_type__in=rule_type)
    if rule_names_to_filter:
        Qs = None
        for rn in rule_names_to_filter:
            if not Qs:
                Qs = Q(**{f"{rn}__sqls__nin": [None, []]}) | \
                     Q(**{f"{rn}__records__nin": [None, []]})
            else:
                Qs = Qs | \
                     Q(**{f"{rn}__sqls__nin": [None, []]}) | \
                     Q(**{f"{rn}__records__nin": [None, []]})
        if Qs:
            result_q = result_q.filter(Qs)
    if schema_name:
        result_q = result_q.filter(schema_name__in=schema_name)
    if cmdb_id:
        result_q = result_q.filter(cmdb_id__in=cmdb_id)
    if cmdb_id_schema_name_pairs:
        Qs = None
        for the_cmdb_id, the_schema_name in cmdb_id_schema_name_pairs:
            if isinstance(the_schema_name, str):
                current_q = Q(cmdb_id=the_cmdb_id, schema_name=the_schema_name)
            elif isinstance(the_schema_name, (tuple, list)):
                current_q = Q(cmdb_id=the_cmdb_id, schema_name__in=the_schema_name)
            else:
                assert 0
            if not Qs:
                Qs = current_q
            else:
                Qs = Qs | current_q
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
                sql_id_set.add((result.cmdb_id, sql["sql_id"]))
    return len(sql_id_set)


def calc_problem_num(result_q, rule_name: Union[str, list, tuple] = None) -> int:
    """
    计算result的query set的问题发生次数
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
            # 首先尝试获取OBJ类型
            records = result_rule_dict.get("records", [])
            if not records:
                # OBJ类型获取失败，则试试SQL类型
                records = result_rule_dict.get("sqls", [])
                if not records:
                    continue
            count += len(records)
    return count


def get_object_unique_labels(
        result_q, rule_name: Union[str, list, tuple] = None) -> list:
    """
    活得某个result queryset内全部的对象唯一标识（表名，索引名，序列名）
    :param result_q:
    :param rule_name:
    :return:
    """
    object_names: set = set()
    if isinstance(rule_name, str):
        rule_name = [rule_name]
    for result in result_q:
        if not rule_name:
            rule_name_to_loop = dir(result)
        else:
            rule_name_to_loop = rule_name
        for rn in rule_name_to_loop:
            the_rule = Rule.filter_enabled(rule_name=rn).first()
            result_rule_dict = getattr(result, rn, None)
            if not result_rule_dict or not isinstance(result_rule_dict, dict):
                continue
            # 尝试获取OBJ类型
            records = result_rule_dict.get("records", [])
            for record in records:
                object_name = the_rule.get_object_name(record, the_rule.obj_info_type)
                if object_name:
                    object_names.add((result.cmdb_id, result.schema_name, object_name))
    return list(object_names)
