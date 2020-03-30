import re


def code(rule, entries, **kwargs):
    single_sql: dict = kwargs.get("single_sql")
    sql_text: str = single_sql["sql_text_no_comment"]

    pat = re.compile(r"\s+union\s+", re.I+re.M)

    if pat.findall(sql_text):
        return -rule.weight, []
    return None, []


code_hole.append(code)
