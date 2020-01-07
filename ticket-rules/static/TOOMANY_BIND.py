def code(rule, **kwargs):
    single_sql: dict = kwargs.get("single_sql")
    sql_text: str = single_sql["sql_text"]

    if sql_text.count(":") > rule.gip("num_of_bound_var"):
        return -rule.weight, []
    return None, []


code_hole.append(code)
