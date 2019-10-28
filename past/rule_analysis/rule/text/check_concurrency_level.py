# Author: kk.Fang(fkfkbill@gmail.com)

import re


def execute_rule(sql, username, etl_date_key, etl_date, **kwargs):

    if not re.search(r"create\s+table", sql, re.I):
        return True, None

    if 'parallel' not in sql:
        return True, None

    res = re.search("parallel\s+(\d)", sql, re.I)
    if res and int(res.group(1)) > 1:
        return False, "表，索引不能设置并行度"

    return True, None


