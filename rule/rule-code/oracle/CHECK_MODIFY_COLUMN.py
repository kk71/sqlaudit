import re


def code(rule, entries, **kwargs):
    single_sql: dict = kwargs.get("single_sql")
    sql_text: str = single_sql["sql_text_no_comment"]

    if re.search(r'alter\s+table\s+.+\s+modify', sql_text, re.I+re.M):
        yield single_sql


code_hole.append(code)
