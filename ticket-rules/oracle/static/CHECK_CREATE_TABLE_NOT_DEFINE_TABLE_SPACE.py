import re


def code(rule, **kwargs):
    single_sql: dict = kwargs.get("single_sql")
    sql_text: str = single_sql["sql_text"]

    if re.search('create\s+table', sql_text, re.I) and 'tablespace' not in sql_text:
        return -rule.weight, []
    return None, []


code_hole.append(code)
