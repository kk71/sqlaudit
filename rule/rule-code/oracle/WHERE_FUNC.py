import re


def code(rule, entries, **kwargs):
    single_sql: dict = kwargs.get("single_sql")
    sql_text: str = single_sql["sql_text_no_comment"]

    where_func = re.compile(r"where\s+.*?[+\-*/()].*[<>=]{1,2}?", re.I+re.S)

    if where_func.search(sql_text):
        yield single_sql


code_hole.append(code)
