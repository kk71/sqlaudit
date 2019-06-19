# Author: kk.Fang(fkfkbill@gmail.com)

from collections import defaultdict
from typing import *

from sqlalchemy.orm.session import Session

from utils.const import *
from utils.perf_utils import timing
from models.mongo import *
from models.oracle import *
from utils import rule_utils
from utils.cache_utils import *


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
        if rule_result:
            if result.rule_type == RULE_TYPE_OBJ:
                rule_name_to_detail[rule_object.rule_name]["violated_num"] += \
                    len(rule_result.get("records", []))

            elif result.rule_type in [
                RULE_TYPE_TEXT,
                RULE_TYPE_SQLSTAT,
                RULE_TYPE_SQLPLAN]:
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
@cache_it(cache=sc, type_to_exclude=Session)
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
    assert perspective in const.ALL_OVERVIEW_ITEM
    assert score_by in const.ALL_SCORE_BY

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
            if perspective == const.OVERVIEW_ITEM_RADAR:
                scores_by_sth[rule_type][schema] += score
            elif perspective == const.OVERVIEW_ITEM_SCHEMA:
                scores_by_sth[schema][rule_type] += score
    calc_score_by.tik("end calcing scores")

    for persp_1, persp_2_score_dict in scores_by_sth.items():
        ret[persp_1] = None
        if score_by == const.SCORE_BY_LOWEST:
            scores_sorted = [s for s in
                             sorted(persp_2_score_dict.items(), key=lambda k: k[1]) if s]
            if scores_sorted:
                ret[persp_1] = scores_sorted[0][1]

        elif score_by == const.SCORE_BY_AVERAGE:
            persp_2_num = len(persp_2_score_dict)
            if persp_2_num:
                ret[persp_1] = sum(persp_2_score_dict.values()) / persp_2_num

    return ret

