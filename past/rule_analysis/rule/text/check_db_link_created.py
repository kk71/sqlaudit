# Author: kk.Fang(fkfkbill@gmail.com)

import re

from .utils import judge_if_ddl


def execute_rule(sql, db_model=None, **kwargs):

    if not judge_if_ddl(sql):
        return False

    if re.search('create\s+database\s+link', sql, re.I):
        return "不建议创建DB LINK"
    return True

