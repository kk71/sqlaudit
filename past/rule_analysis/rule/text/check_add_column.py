# Author: kk.Fang(fkfkbill@gmail.com)

import re

from .utils import judge_if_ddl


def execute_rule(sql, db_model=None, **kwargs):
    if not judge_if_ddl(sql):
        return False

    if re.search('alter\s+table\s+.+\s+add', sql, re.I):
        if not re.search(".+\s+default\s+.+", sql, re.I):
            return True

    return False