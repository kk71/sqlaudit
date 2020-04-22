import re


def execute_rule(**kwargs):
    sql = kwargs.get("sql")
    re_one = re.compile(r"where\s+.*?[+\-*/].*[<>=]{1,2}?", re.I + re.S)
    where_func_r = re_one.search(sql)
    if where_func_r:
        re_where_fun = where_func_r.group()
        re_two = re.compile(r"^where\s+.*(?=(\(\*\)|\(\+\)))", re.I + re.S)
        if not re.search(re_two, re_where_fun):
            return True
    return False