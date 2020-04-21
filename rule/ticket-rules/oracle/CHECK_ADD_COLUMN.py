import re


def code(rule, entries, **kwargs):
    single_sql: dict = kwargs.get("single_sql")
    sql_text: str = single_sql["sql_text_no_comment"]

    if re.search(r'alter\s+table\s+.+\s+add', sql_text, re.I):
        if not re.search(r".+\s+default\s+.+", sql_text, re.I):
            yield {}


code_hole.append(code)