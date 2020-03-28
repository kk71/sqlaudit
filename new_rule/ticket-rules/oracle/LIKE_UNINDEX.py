import re


def code(rule, entries, **kwargs):
    single_sql: dict = kwargs.get("single_sql")
    sql_text: str = single_sql["sql_text_no_comment"]

    like_unindex = re.compile("like .\\%")

    if like_unindex.search(sql_text):
        return -rule.weight, []
    return None, []


code_hole.append(code)
