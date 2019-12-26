import re


def code(**kwargs):
    single_sql: dict = kwargs.get("single_sql")
    sql_text: str = single_sql["sql_text"]

    sql_content = [
        x
        for x in re.findall(r" in\s*\((.*?)\)", sql_text, re.I)
        if 'select' not in x
    ]
    for value in sql_content:
        if value.count(",") > rule.gip("in_list_num") - 1:
            return RULE_MINUS_DEFAULT, []
    return None, []

code_hole.append(code)
