import re


def code(rule, entries, **kwargs):
    single_sql: dict = kwargs.get("single_sql")
    sql_text: str = single_sql["sql_text_no_comment"]

    dml_sort = re.compile(r"(\s)?((update )|(delete )).*order by", re.M+re.I)

    if dml_sort.search(sql_text):
        yield {}


code_hole.append(code)
