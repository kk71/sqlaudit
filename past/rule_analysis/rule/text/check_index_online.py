# Author: kk.Fang(fkfkbill@gmail.com)

import re

from .utils import judge_if_ddl


def execute_rule(sql, db_model=None, **kwargs):

    if not judge_if_ddl(sql):
        return False

    if re.search(r'create\s+index\s+', sql, re.I):
        if not re.search(r"\s+online", sql, re.I):
            return True
    return False

