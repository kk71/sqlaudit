# Author: kk.Fang(fkfkbill@gmail.com)

import re


def judge_if_ddl(sql):
    """
    判断是不是DDL语句
    :param sql:
    :return:
    """
    if re.match('drop|create|alter|truncate|revoke', sql, flags=re.I):
        return True

    return False
