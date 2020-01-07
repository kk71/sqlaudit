import re


def judge_ddl(sql_text):
    """
    判断是不是DDL语句
    :param sql_text:
    :return:
    """
    from utils.const import SQL_KEYWORDS, SQL_DDL

    ddl = "|".join(SQL_KEYWORDS[SQL_DDL])

    if re.findall(r'^\s*' + '(' + ddl + ')', sql_text, flags=re.I | re.M):
        return True
    return False
