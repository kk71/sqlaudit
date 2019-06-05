# Author: kk.Fang(fkfkbill@gmail.com)

import re
import json

from mongoengine import Q

from models.oracle import RiskSQLRule
from models.mongo import Rule, Results


# 业务类型

MODEL_OLTP = "OLTP"
MODEL_OLAP = "OLAP"
ALL_SUPPORTED_MODEL = (MODEL_OLAP, MODEL_OLTP)

# 规则状态

RULE_STATUS_ON = "ON"
RULE_STATUS_OFF = "OFF"
ALL_RULE_STATUS = (RULE_STATUS_ON, RULE_STATUS_OFF)

# 规则类型

RULE_TYPE_OBJ = "OBJ"
RULE_TYPE_TEXT = "TEXT"
RULE_TYPE_SQLPLAN = "SQLPLAN"
RULE_TYPE_SQLSTAT = "SQLSTAT"
ALL_RULE_TYPE = (RULE_TYPE_OBJ, RULE_TYPE_TEXT, RULE_TYPE_SQLPLAN, RULE_TYPE_SQLSTAT)

# 定位一条规则的字段们

RULE_ALLOCATING_KEYS = ("db_type", "db_model", "rule_name")


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


def get_rules_dict() -> dict:
    """
    parse all rules into a dict with 3-key indexing
    """
    # TODO make it cached
    return {(r.db_type, r.db_model, r.rule_name): r for r in Rule.objects().all()}


def calc_sum_of_rule_max_score(db_type, db_model, rule_type) -> float:
    """
    计算某个类型的规则的最大分总合
    """
    # TODO make it cached
    rule_q = Rule.objects(db_type=db_type, db_model=db_model, rule_type=rule_type)
    return sum([float(rule.max_score) for rule in rule_q])


def get_risk_rules_dict(session) -> dict:
    """
    parse all risk rules into a dict with 3-key indexing
    """
    # TODO make it cached
    risk_rule_list = list(session.query(RiskSQLRule).filter_by().all())
    return {(r.db_type, r.db_model, r.rule_name): r for r in risk_rule_list}


def get_all_risk_towards_a_sql(session, sql_id, date_range: tuple):
    """
    用当前配置的风险规则，去遍历results
    :param session:
    :param sql_id:
    :param date_range: a tuple of two datetime objects
    :return:
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
    return list(session.query(RiskSQLRule).
                filter(RiskSQLRule.rule_name.in_(list(rule_name_set))).
                       with_entities(RiskSQLRule.risk_sql_rule_id))
