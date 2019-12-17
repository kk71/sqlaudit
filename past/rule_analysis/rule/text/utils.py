# Author: kk.Fang(fkfkbill@gmail.com)

import re


def judge_if_ddl(sql):
    """
    判断是不是DDL语句
    :param sql:
    :return:
    """
    from utils.const import SQL_KEYWORDS

    d_l="|".join(SQL_KEYWORDS[1])

    if re.findall(r'^\s*'+'('+d_l+')', sql, flags=re.I | re.M):
        return True

    return False
