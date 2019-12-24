

import re


def execute_rule(sql, db_model=None, **kwargs):

    if re.search('@', sql, re.I):

        return True
    return False