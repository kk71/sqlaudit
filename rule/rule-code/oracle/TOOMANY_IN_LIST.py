import re


def code(rule, entries, **kwargs):
    single_sql: dict = kwargs["single_sql"]
    sql_text: str = single_sql["sql_text_no_comment"]

    sql_content = [
        x
        for x in re.findall(r"\s+in\s+\((.*?)\)", sql_text, re.I+re.S)
        if 'select' not in x.lower()
    ]
    for value in sql_content:
        if value.count(",") >= rule.gip("in_list_num") - 1:
            yield {}


code_hole.append(code)
