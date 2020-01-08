import re


def code(rule, **kwargs):
    single_sql: dict = kwargs.get("single_sql")
    sql_text: str = single_sql["sql_text"]

    pat = re.compile("union")
    pat_all = re.compile("union all")

    if pat.search(sql_text) and not pat_all.search(sql_text):
        return -rule.weight, []
    return None, []


code_hole.append(code)
