import re


def code(rule, **kwargs):
    single_sql: dict = kwargs.get("single_sql")
    sql_text: str = single_sql["sql_text"]

    if re.search(r'truncate\s+table\s+', sql_text, re.I):
        return -rule.weight, []
    return None, []


code_hole.append(code)
