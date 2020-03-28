import re


def code(rule, entries, **kwargs):
    single_sql: dict = kwargs.get("single_sql")
    sql_text: str = single_sql["sql_text_no_comment"]

    if re.search(r'create\s+index', sql_text, re.I) and 'tablespace' not in sql_text:
        return -rule.weight, []
    return None, []


code_hole.append(code)
