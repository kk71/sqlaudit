def code(rule, **kwargs):
    single_sql: dict = kwargs.get("single_sql")
    sql_text: str = single_sql["sql_text_no_comment"]

    m = 0
    n = 0
    sql_content = []
    sqlbegin = 0
    sqlend = 0
    str_len = len(sql_text)
    for k in range(str_len):
        if sql_text[k] == "(":
            m = m + 1
        if sql_text[k] == ")":
            m = m - 1
        if sql_text[k: k + 6] == "having" and m == 0:
            sqlbegin = k + 7
            n = n + 1
        if k == str_len - 1 and n > 0 and m == 0:
            sqlend = k - 1
            sql_content.append(sql_text[sqlbegin:sqlend])
    for value in sql_content:
        if "select" in value:
            return -rule.weight, []
    return None, []


code_hole.append(code)