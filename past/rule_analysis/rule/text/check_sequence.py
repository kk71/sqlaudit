# Author: kk.Fang(fkfkbill@gmail.com)

import re


def execute_rule(sql, db_model, **kwargs):
    if not re.search(r"create\s+sequence", sql, re.I):
        return True

    if ' order' in sql:
        return "使用了order的序列"
    if ' cache ' not in sql:
        return "没有显式指定cache并且序列的cache需要指定为2000或以上"
    res = re.search(r"cache\s+(\d+)", sql, re.I)
    if res and int(res.group(1)) < 2000:
        return "序列的cache需要指定为2000或以上"

    return True
