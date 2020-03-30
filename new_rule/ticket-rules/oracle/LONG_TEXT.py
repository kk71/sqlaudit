def code(rule, entries, **kwargs):
    single_sql: dict = kwargs.get("single_sql")
    sql_text: str = single_sql["sql_text"]

    if len(sql_text) > rule.gip("char_num"):
        return -rule.weight, []
    return None, []


code_hole.append(code)
