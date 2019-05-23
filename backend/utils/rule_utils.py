# Author: kk.Fang(fkfkbill@gmail.com)

import re
import json

from backend.models.mongo import *


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
RULE_TYPE_SQLPLAN = "SQLPAN"
RULE_TYPE_SQLSTAT = "SQLSTAT"
ALL_RULE_TYPE = (RULE_TYPE_OBJ, RULE_TYPE_TEXT, RULE_TYPE_SQLPLAN, RULE_TYPE_SQLSTAT)

# 定位一条规则的字段们

RULE_ALLOCATING_KEYS = ("db_type", "db_model", "rule_name", "rule_type")


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
    :param risk_rule_keys:
    :param rule_keys:
    """
    risk_rule_dict = risk_rule_object.to_dict()
    if not rule_object:
        rule_object = Rule.objects(**risk_rule_object.
                                   to_dict(iter_if=lambda k, v: k in RULE_ALLOCATING_KEYS)).first()
    rule_dict = rule_object.to_dict(iter_if=lambda k, v: k in rule_keys)
    return {**risk_rule_dict, **rule_dict}
