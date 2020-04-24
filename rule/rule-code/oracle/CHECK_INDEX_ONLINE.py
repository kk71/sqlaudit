import re


def code(rule, entries, **kwargs):
    single_sql: dict = kwargs.get("single_sql")
    sql_text: str = single_sql["sql_text_no_comment"]

    if re.search(r'create\s+index\s+', sql_text, re.I+re.M):
        if not re.search(r"\s+online", sql_text, re.I+re.M):
            yield {"sql_id": kwargs.get("sql_id", None)}


code_hole.append(code)
