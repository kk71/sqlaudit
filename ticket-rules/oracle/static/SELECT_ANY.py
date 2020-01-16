import re


def code(rule, **kwargs):
    single_sql: dict = kwargs.get("single_sql")
    sql_text: str = single_sql["sql_text_no_comment"]

    select_any = re.compile("(select \\*)|(select .*\\.\\*)")

    if re.search(select_any, sql_text):
        return -rule.weight, []
    return None, []


code_hole.append(code)
