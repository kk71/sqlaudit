import re


def code(rule, entries, **kwargs):
    single_sql: dict = kwargs.get("single_sql")
    sql_text: str = single_sql["sql_text_no_comment"]

    if not re.search(r"create\s+sequence", sql_text, re.I+re.M):
        return

    res = re.search(r"cache\s+(\d+)", sql_text, re.I)
    if 'order' in sql_text.lower() or\
            'cache' not in sql_text.lower() or\
            (res and int(res.group(1)) < 2000):
        yield {}


code_hole.append(code)
