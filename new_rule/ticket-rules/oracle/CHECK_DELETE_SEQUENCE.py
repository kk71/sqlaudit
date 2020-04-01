import re


def code(rule, entries, **kwargs):
    single_sql: dict = kwargs.get("single_sql")
    sql_text: str = single_sql["sql_text_no_comment"]

    if re.search(r'drop\s+sequence', sql_text, re.I):
        yield {}


code_hole.append(code)
