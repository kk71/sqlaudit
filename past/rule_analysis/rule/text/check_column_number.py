# Author: kk.Fang(fkfkbill@gmail.com)

import re

from .utils import judge_if_ddl


def execute_rule(sql, db_model=None, **kwargs):

    if not judge_if_ddl(sql):
        return False

    if not re.search(r"create\s+table", sql, re.I):
        return True

    left_brackets = 0
    left_flag = 0
    right_flag = 0
    for index, s in enumerate(sql):
        if s == "(":
            left_brackets += 1
            if left_brackets == 1:
                left_flag = index
        elif s == ")":
            left_brackets -= 1
            if left_brackets == 0:
                right_flag = index
                break

    sql = sql[left_flag + 1: right_flag]

    if len(sql.split(',')) > 255:
        return "表字段个数不能超过255"
    return True
