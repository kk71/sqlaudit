def code(rule, entries, **kwargs):
    single_sql: dict = kwargs.get("single_sql")
    sql_text: str = single_sql["sql_text_no_comment"]

    if sql_text.count(":") >= rule.gip("num_of_bound_var"):
        yield single_sql


code_hole.append(code)
