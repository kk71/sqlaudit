import re


def code(rule, **kwargs):
    single_sql: dict = kwargs.get("single_sql")
    sql_text: str = single_sql["sql_text"]

    pat = re.compile('(\s)?((update )|(delete ))')
    pat1 = re.compile(' where ')

    if pat.search(sql_text) and not pat1.search(sql_text):
        return -rule.weight, []
    return None, []


code_hole.append(code)
