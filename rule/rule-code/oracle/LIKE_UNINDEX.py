import re


def code(rule, entries, **kwargs):
    single_sql: dict = kwargs.get("single_sql")
    sql_text: str = single_sql["sql_text_no_comment"]

    like_unindex = re.compile(r"like\s+\.?%", re.M+re.I)

    if like_unindex.search(sql_text):
        yield single_sql


code_hole.append(code)
