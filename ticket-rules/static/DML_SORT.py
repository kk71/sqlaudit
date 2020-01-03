import re


def code(rule, **kwargs):
    single_sql: dict = kwargs.get("single_sql")
    sql_text: str = single_sql["sql_text"]

    dml_sort = re.compile("(\\s)?((update )|(delete )).*order by")

    if dml_sort.search(sql_text):
        return -rule.weight, []
    return None, []


code_hole.append(code)
