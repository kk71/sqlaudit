def code(rule, **kwargs):
    """重复查询子句"""
    single_sql: dict = kwargs.get("single_sql")
    sql_text: str = single_sql["sql_text_no_comment"]

    str_len = len(sql_text)
    left_bracket = []
    sql_content = []
    subquery = []

    for i in range(str_len):
        if sql_text[i] == "(":
            left_bracket.append(i)
        if sql_text[i] == ")":
            start = sum(left_bracket[-1:-2:-1]) + 1
            stop = i - 1
            sql_content.append(sql_text[start:stop])
    for value in sql_content:
        if "select" in value and value not in subquery:
            subquery.append(value)
        elif "select" in value and value in subquery:
            return -rule.weight, []
    return None, []


code_hole.append(code)
