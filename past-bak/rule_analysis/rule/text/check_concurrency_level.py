# Author: kk.Fang(fkfkbill@gmail.com)

import re

from .utils import judge_if_ddl


def execute_rule(sql, db_model=None, **kwargs):

    if not judge_if_ddl(sql):
        return False

    if not re.search(r"create\s+table", sql, re.I):
        return False

    if 'parallel' not in sql:
        return False

    res = re.search("parallel\s+(\d)", sql, re.I)
    if res and int(res.group(1)) > 1:
        #return "表，索引不能设置并行度"
        return True

    return False


