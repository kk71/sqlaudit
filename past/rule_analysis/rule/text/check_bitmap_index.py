# Author: kk.Fang(fkfkbill@gmail.com)

import re

from .utils import judge_if_ddl


def execute_rule(sql, db_model=None, **kwargs):
    # if not judge_if_ddl(sql):
    #     return False

    if db_model == "OLTP" and re.search("create\s+bitmap\s+index", sql, re.I):
        return True, "不建议创建位图索引"
    return False


