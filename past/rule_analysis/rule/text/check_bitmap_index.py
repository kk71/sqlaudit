# Author: kk.Fang(fkfkbill@gmail.com)

import re


def execute_rule(sql, db_model, **kwargs):

    if db_model == "OLTP" and re.search("create\s+bitmap\s+index", sql, re.I):
        return False, "不建议创建位图索引"
    return True


