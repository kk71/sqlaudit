# Author: kk.Fang(fkfkbill@gmail.com)

import re

from .utils import judge_if_ddl


def execute_rule(sql, db_model=None, **kwargs):

    # if not judge_if_ddl(sql):
    #     return False

    if re.search('create\s+index', sql, re.I) and 'tablespace' not in sql:
        #return "需要为索引指定表空间"
        return True
    return False

