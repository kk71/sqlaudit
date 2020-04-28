import re


def code(rule, entries, **kwargs):
    single_sql: dict = kwargs.get("single_sql")
    sql_text: str = single_sql["sql_text_no_comment"]

    pat = re.compile(r'(\s)?((update )|(delete ))', re.M+re.I)
    pat1 = re.compile(r'\s+where\s+', re.S+re.I)

    if pat.search(sql_text) and not pat1.search(sql_text):
        yield single_sql


code_hole.append(code)
