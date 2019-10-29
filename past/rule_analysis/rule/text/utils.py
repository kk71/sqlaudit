# Author: kk.Fang(fkfkbill@gmail.com)


def judge_if_ddl(sql):
    """
    判断是不是DDL语句
    :param sql:
    :return:
    """
    if 'create' in sql or 'drop' in sql or 'alter' in sql:
        return True
    return False
