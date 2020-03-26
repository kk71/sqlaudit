def code(rule, **kwargs):
    single_sql: dict = kwargs.get("single_sql")
    sql_text: str = single_sql["sql_text_no_comment"]

    str_len = len(sql_text)
    sql_content = []
    sqlbegin = 0
    m = 0
    n = 0
    for k in range(str_len):
        if sql_text[k] == "(":
            m = m + 1
        if sql_text[k] == ")":
            m = m - 1
        if sql_text[k: k + 5] == "where" and m == 0:
            sqlbegin = k + 6
            n = n + 1
        if sql_text[k: k + 6] == "having" and n != 0 and m == 0:
            sqlend = k - 1
            n = n - 1
            sql_content.append(sql_text[sqlbegin:sqlend])
        if k == str_len - 1 and n > 0 and m == 0:
            sqlend = k - 1
            sql_content.append(sql_text[sqlbegin:sqlend])
    for value in sql_content:
        if "select" in value:
            return -rule.weight, []
    return None, []


code_hole.append(code)
