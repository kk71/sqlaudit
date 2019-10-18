# Author: kk.Fang(fkfkbill@gmail.com)

import re


def execute_rule(sql, username, etl_date_key, etl_date, **kwargs):

    if db_model == "OLTP" and re.search("create\s+bitmap\s+index", sql, re.I):
        return False, "不建议创建位图索引"
    return True, None


