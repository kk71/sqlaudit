import re


def code(rule, **kwargs):
    single_sql: dict = kwargs.get("single_sql")
    sql_text: str = single_sql["sql_text"]

    where_not = re.compile("(!=)|(<>)|(!>)|(!<)")

    if re.search(where_not, sql_text):
        return -rule.weight, []
    return None, []


code_hole.append(code)
