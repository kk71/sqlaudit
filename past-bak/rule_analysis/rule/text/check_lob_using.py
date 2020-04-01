# Author: kk.Fang(fkfkbill@gmail.com)

import re

from .utils import judge_if_ddl


def execute_rule(sql, db_model=None, **kwargs):

    if not judge_if_ddl(sql):
        return False

    if not re.search(r"create\s+table", sql, re.I) and not re.search(r"alter\s+table", sql, re.I):
        return False

    if any([x in sql.lower() for x in ['blob', 'clob', 'bfile', 'xmltype']]):
        #return "高频表上不推荐使用LOB字段"
        return True

    return False

