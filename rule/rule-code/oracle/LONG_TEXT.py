def code(rule, entries, **kwargs):
    single_sql: dict = kwargs.get("single_sql")
    sql_text: str = single_sql["sql_text"]

    length = len(sql_text)
    if length >= rule.gip("char_num"):
        yield {
            "char_num": length
        }


code_hole.append(code)
