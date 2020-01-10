import re


def code(rule, **kwargs):
    single_sql: dict = kwargs.get("single_sql")
    sql_text: str = single_sql["sql_text"]

    if not re.search(r"create\s+sequence", sql_text, re.I):
        return None, []

    if 'order' in sql_text:
        return -rule.weight, []
    if 'cache' not in sql_text:
        return -rule.weight, []
    res = re.search(r"cache\s+(\d+)", sql_text, re.I)
    if res and int(res.group(1)) < 2000:  # TODO 这不应写死
        # if res and int(res.group(1)) < rule.gip(""):
        return -rule.weight, []
    return None, []


code_hole.append(code)
