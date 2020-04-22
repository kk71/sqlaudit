import re


def code(rule, entries, **kwargs):
    single_sql: dict = kwargs.get("single_sql")
    sql_text: str = single_sql["sql_text_no_comment"]

    if len(re.compile(r"\s+or\s+", re.I+re.M).findall(sql_text))\
            >= rule.gip("or_num"):
        yield {}


code_hole.append(code)
