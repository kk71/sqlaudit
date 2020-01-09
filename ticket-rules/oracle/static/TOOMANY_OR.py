def code(rule, **kwargs):
    single_sql: dict = kwargs.get("single_sql")
    sql_text: str = single_sql["sql_text"]

    if sql_text.count("or") > rule.gip("or_num"):
        return -rule.weight, []
    return None, []


code_hole.append(code)
