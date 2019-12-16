

import re

from .utils import judge_if_ddl


def execute_rule(sql, db_model=None, **kwargs):

    if not judge_if_ddl(sql):
        return False

    if re.search('create\s+table', sql, re.I) and 'tablespace' not in sql:
        return True
    return False