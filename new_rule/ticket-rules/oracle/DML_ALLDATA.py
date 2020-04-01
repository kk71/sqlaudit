import re


def code(rule, entries, **kwargs):
    single_sql: dict = kwargs.get("single_sql")
    sql_text: str = single_sql["sql_text_no_comment"]

    pat = re.compile(r'(\s)?((update )|(delete ))')
    pat1 = re.compile(' where ')

    if pat.search(sql_text) and not pat1.search(sql_text):
        yield {}


code_hole.append(code)
