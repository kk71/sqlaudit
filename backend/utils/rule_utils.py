# Author: kk.Fang(fkfkbill@gmail.com)

import re


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
