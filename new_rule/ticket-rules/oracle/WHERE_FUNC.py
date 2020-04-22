import re


def code(rule, entries, **kwargs):
    single_sql: dict = kwargs.get("single_sql")
    sql_text: str = single_sql["sql_text_no_comment"]
    re_one = re.compile(r"where\s+.*?[+\-*/].*[<>=]{1,2}?",re.I+re.S)
    where_func_r=re_one.search(sql_text)
    if where_func_r:
        re_where_fun=where_func_r.group()
        re_two=re.compile(r"^where\s+.*(?=(\(\*\)|\(\+\)))",re.I+re.S)
        if not re.search(re_two,re_where_fun):
            return -rule.weight, []
    return None, []


code_hole.append(code)
