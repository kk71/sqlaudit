# Author: kk.Fang(fkfkbill@gmail.com)

import re


def execute_rule(sql, db_model, **kwargs):

    if re.search('create\s+index', sql, re.I) and 'tablespace' not in sql:
        return False, "需要为索引指定表空间"
    return True, None

