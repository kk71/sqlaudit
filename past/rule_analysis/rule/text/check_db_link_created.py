# Author: kk.Fang(fkfkbill@gmail.com)

import re


def execute_rule(sql, db_model, **kwargs):

    if re.search('create\s+database\s+link', sql, re.I):
        return False, "不建议创建DB LINK"
    return True, None

