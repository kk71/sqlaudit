import re


def code(rule, entries, **kwargs):
    single_sql: dict = kwargs.get("single_sql")
    sql_text: str = single_sql["sql_text_no_comment"]

    if not re.search(r"create\s+table", sql_text, re.I+re.M):
        return

    if 'parallel' not in sql_text.lower():
        return
    parallel = re.search(r"parallel\s+(\d)", sql_text, re.I+re.M)
    if parallel and int(parallel.group(1)) > 1:
        yield {"sql_id": kwargs.get("sql_id", None)}


code_hole.append(code)
