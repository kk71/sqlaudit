import re


def code(rule, entries, **kwargs):
    single_sql: dict = kwargs.get("single_sql")
    sql_text: str = single_sql["sql_text_no_comment"]

    if re.search(r'alter\s+table\s+.+\s+add', sql_text, re.I+re.M):
        if not re.search(r".+\s+default\s+.+", sql_text, re.I+re.M):
            yield single_sql


code_hole.append(code)
