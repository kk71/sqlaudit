# Author: kk.Fang(fkfkbill@gmail.com)

import re
import json

from mongoengine import Q

from models.oracle import RiskSQLRule
from models.mongo import *
from utils.perf_utils import *
from utils.const import *


def text_parse(key, rule_complexity, rule_cmd, input_params, sql):
    """sql文本规则的读取"""
    args = {param["parm_name"]: param["parm_value"] for param in input_params}
    violate = False
    # 解析简单规则
    if rule_complexity == "simple" and re.search(rule_cmd, sql):
        violate = True
    elif rule_complexity == "complex":
        module_name = ".".join(["rule_analysis.rule.text", key.lower()])
        module = __import__(module_name, globals(), locals(), "execute_rule")
        if module.execute_rule(sql=sql, **args):
            violate = True
    return violate


def format_rule_result_detail(rule_object, record: list):
    """
    格式化输出规则分析结果(即风险详情)的信息。信息来源与mongodb.results.(rule_name).records
    :return:
    """
    output_params = rule_object.output_parms
    risk_detail = ', '.join([': '.join([
        output_params[index]['parm_desc'], str(value)]) for index, value in enumerate(record)])
    return risk_detail


def import_from_json_file(filename: str):
    """
    从json文件导入规则至mongodb
    :param filename:
    :return: 导入数, 总共数
    """
    with open(filename, "r") as z:
        rules = json.load(z)
    rules_to_import = []
    for rule in rules:
        the_rule = Rule()
        the_rule.from_dict(rule, iter_if=lambda k, v: k not in ("_id", ))
        rules_to_import.append(the_rule)
    Rule.objects.insert(rules_to_import)
    return len(rules_to_import), len(rules)


@timing
def merge_risk_rule_and_rule(
        risk_rule_object, rule_object=None, rule_keys=("rule_desc", "rule_name")) -> dict:
    """
    merge rule_object to risk_rule object
    :param risk_rule_object:
    :param rule_object:
    :param rule_keys:
    """
    risk_rule_dict = risk_rule_object.to_dict()
    if not rule_object:
        rule_object = Rule.objects(**risk_rule_object.
                                   to_dict(iter_if=lambda k, v: k in RULE_ALLOCATING_KEYS)).first()
    rule_dict = rule_object.to_dict(iter_if=lambda k, v: k in rule_keys)
    return {**risk_rule_dict, **rule_dict}


def get_rules_dict(rule_status: str = RULE_STATUS_ON) -> dict:
    """
    parse all rules into a dict with 3-key indexing
    :param rule_status:
    :return:
    """
    # TODO make it cached
    return {(r.db_type, r.db_model, r.rule_name): r for r in
            Rule.objects(rule_status=rule_status).all()}


@timing
def calc_sum_of_rule_max_score(db_type, db_model, rule_type) -> float:
    """
    计算某个类型的规则的最大分总合
    """
    # TODO make it cached
    rule_q = Rule.filter_enabled(db_type=db_type, db_model=db_model, rule_type=rule_type)
    return sum([float(rule.max_score) for rule in rule_q])


def get_risk_rules_dict(session) -> dict:
    """
    parse all risk rules into a dict with 3-key indexing
    """
    # TODO make it cached
    risk_rule_list = list(session.query(RiskSQLRule).filter_by().all())
    return {(r.db_type, r.db_model, r.rule_name): r for r in risk_rule_list}


@timing
def get_all_risk_towards_a_sql(session, sql_id, db_model: str, date_range: tuple) -> set:
    """
    用当前配置的风险规则，去遍历results
    :param session:
    :param sql_id:
    :param db_model:
    :param date_range: a tuple of two datetime objects
    :return: risk rule_name set
    """
    risk_rule_dict = get_risk_rules_dict(session)
    all_risk_rule_name_list = [i[2]  # 0db_type 1 db_model 2rule_name
                                 for i in risk_rule_dict.keys()]
    result_q = Results.objects()
    date_start, date_end = date_range
    if date_start:
        result_q = result_q.filter(create_date__gte=date_start)
    if date_end:
        result_q = result_q.filter(create_date__lte=date_end)
    Qs = None
    for rn in all_risk_rule_name_list:
        if not Qs:
            Qs = Q(**{f"{rn}__sqls__sql_id": sql_id})
        else:
            Qs = Qs | Q(**{f"{rn}__sqls__sql_id": sql_id})
    result_q = result_q.filter(Qs)
    rule_name_set = set()
    for r in result_q:
        for rn in all_risk_rule_name_list:
            if getattr(r, rn, None) and getattr(r, rn).get("sqls"):
                for sql in getattr(r, rn)["sqls"]:
                    if sql["sql_id"] == sql_id:
                        rule_name_set.add(rn)
    return rule_name_set


@timing
def get_risk_rate(cmdb_id, date_range: tuple) -> dict:
    """
    获取最近的风险率
    :param cmdb_id:
    :param date_range:
    :return:
    """
    # TODO make it cached
    date_start, date_end = date_range
    results_q = Results.objects(cmdb_id=cmdb_id, create_date__gte=date_start,
                                create_date__lte=date_end)
    # OBJ
    tab_info_q = ObjTabInfo.objects(cmdb_id=cmdb_id)
    part_tab_info_q = ObjPartTabParent.objects(cmdb_id=cmdb_id)
    index_q = ObjIndColInfo.objects(cmdb_id=cmdb_id)
    # others
    sql_text_q = SQLText.objects(cmdb_id=cmdb_id)
    sql_plan_q = MSQLPlan.objects(cmdb_id=cmdb_id)
    sql_stats_q = SQLStat.objects(cmdb_id=cmdb_id)
    # rule dict for searching obj_info_type
    rule_dict = get_rules_dict()
    ret = {
        # OBJ
        "table": {"violation_num": 0, "sum": 0, "rate": 0.0},  # table包括普通表及分区表,无属性或类型可定义，故以table表示
        "sequence": {"violation_num": 0, "sum": 0, "rate": 0.0},
        OBJ_RULE_TYPE_INDEX: {"violation_num": 0, "sum": 0, "rate": 0.0},
        # others
        RULE_TYPE_TEXT: {"violation_num": 0, "sum": 0, "rate": 0.0},
        RULE_TYPE_SQLSTAT: {"violation_num": 0, "sum": 0, "rate": 0.0},
        RULE_TYPE_SQLPLAN: {"violation_num": 0, "sum": 0, "rate": 0.0}
    }
    record_id_set: set = set()
    for result in results_q:
        for rule_3k, rule_obj in rule_dict.items():
            result_rule_dict = getattr(result, rule_3k[2], None)
            if not result_rule_dict:
                continue
            if result.rule_type == RULE_TYPE_OBJ:
                records = result_rule_dict["records"]
                if rule_obj.obj_info_type in (OBJ_RULE_TYPE_TABLE, OBJ_RULE_TYPE_PART_TABLE):
                    ret["table"]["violation_num"] += len(records)
                elif rule_obj.obj_info_type == OBJ_RULE_TYPE_INDEX:
                    ret[OBJ_RULE_TYPE_INDEX]["violation_num"] += len(records)
            else:
                sqls = result_rule_dict["sqls"]
                ret[result.rule_type]["violation_num"] += len(sqls)
        record_id_set.add(result.record_id)
    for record_id in record_id_set:
        # table include normal table and part-table, so calc twice.
        ret["table"]["sum"] += tab_info_q.filter(record_id=record_id).count()
        ret["table"]["sum"] += part_tab_info_q.filter(record_id=record_id).count()
        # OBJ index
        ret[OBJ_RULE_TYPE_INDEX]["sum"] += index_q.filter(record_id=record_id).count()
        # other
        ret[RULE_TYPE_TEXT]["sum"] += sql_text_q.filter(record_id=record_id).count()
        ret[RULE_TYPE_SQLSTAT]["sum"] += sql_stats_q.filter(record_id=record_id).count()
        ret[RULE_TYPE_SQLPLAN]["sum"] += sql_plan_q.filter(record_id=record_id).count()
    for k, v in ret.items():
        if v["sum"] and v["violation_num"]:
            v["rate"] = float(v["violation_num"]) / v["sum"]
    return ret


@timing
def get_score_of_4_perspective():
    """
    获取四个维度的评分
    :return:
    """
    return
