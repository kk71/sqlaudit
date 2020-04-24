import re


def code(rule, entries, **kwargs):
    single_sql: dict = kwargs.get("single_sql")
    sql_text: str = single_sql["sql_text_no_comment"]

    where_not = re.compile("(!=)|(<>)|(!>)|(!<)")

    if re.search(where_not, sql_text):
        yield {"sql_id": kwargs.get("sql_id", None)}


code_hole.append(code)
