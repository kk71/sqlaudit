import re


def code(rule, entries, **kwargs):
    """列数"""
    single_sql: dict = kwargs.get("single_sql")
    sql_text: str = single_sql["sql_text_no_comment"]

    if not re.search(r"create\s+table", sql_text, re.I):
        return None, []

    left_brackets = 0
    left_flag = 0
    right_flag = 0
    for index, s in enumerate(sql_text):
        if s == "(":
            left_brackets += 1
            if left_brackets == 1:
                left_flag = index
        elif s == ")":
            left_brackets -= 1
            if left_brackets == 0:
                right_flag = index
                break

    sql = sql_text[left_flag + 1: right_flag]

    if len(sql.split(',')) > 255:  # TODO,这不应写死
        # if len(sql.split(',')) > rule.gip(""):
        return -rule.weight, []
    return None, []


code_hole.append(code)
