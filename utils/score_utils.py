# Author: kk.Fang(fkfkbill@gmail.com)

from collections import defaultdict

from utils.const import *
from utils.perf_utils import timing
from models.mongo import *
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
        if rule_result:
            if result.rule_type == RULE_TYPE_OBJ:
                rule_name_to_detail[rule_object.rule_name]["violated_num"] += \
                    len(rule_result.get("records", []))

            if result.rule_type in [
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


@timing
def calc_score_of_rule_type(cmdb_id, db_model) -> dict:
    """
    获取某个纳管数据库最后一次的按照规则类型分类的评分
    :param cmdb_id:
    :param db_model:
    :return:
    """
    # TODO make it cached
    lastest_result = Results.objects(cmdb_id=cmdb_id).order_by("-create_date").first()
    job_id = lastest_result.task_uuid
    ret = {
        RULE_TYPE_OBJ: 0.0,
        RULE_TYPE_TEXT: 0.0,
        RULE_TYPE_SQLPLAN: 0.0,
        RULE_TYPE_SQLSTAT: 0.0
    }
    for result in results_q:
        _, scores = calc_result(result, db_model)
        ret[result.rule_type] += scores
    return ret
