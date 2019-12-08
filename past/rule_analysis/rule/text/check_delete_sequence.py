# Author: kk.Fang(fkfkbill@gmail.com)

import re

from .utils import judge_if_ddl


def execute_rule(sql, db_model=None, **kwargs):
    if not judge_if_ddl(sql):
        return False

    if re.search('drop\s+sequence', sql, re.I):
        return True

    return False
