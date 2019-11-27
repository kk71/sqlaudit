# Author: kk.Fang(fkfkbill@gmail.com)

import re

from .utils import judge_if_ddl


def execute_rule(sql, db_model=None, **kwargs):

    if not judge_if_ddl(sql):
        return False

    if not re.search(r"create\s+sequence", sql, re.I):
        return False

    if ' order' in sql:
        #return "使用了order的序列"
        return True
    if ' cache ' not in sql:
        # return "没有显式指定cache并且序列的cache需要指定为2000或以上"
        return True
    res = re.search(r"cache\s+(\d+)", sql, re.I)
    if res and int(res.group(1)) < 2000:
        # return "序列的cache需要指定为2000或以上"
        return True

    return False
