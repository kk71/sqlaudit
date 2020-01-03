import re


def code(rule, **kwargs):
    single_sql: dict = kwargs.get("single_sql")
    sql_text: str = single_sql["sql_text"]

    where_func = re.compile("\\)\\s?[<>=]{1,2}")

    if where_func.search(sql_text):
        return -rule.weight, []
    return None, []


code_hole.append(code)
