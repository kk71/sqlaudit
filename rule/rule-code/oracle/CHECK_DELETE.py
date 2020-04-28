from parsed_sql.parsed_sql import ParsedSQL


def code(rule, entries, **kwargs):
    single_sql: dict = kwargs.get("single_sql")
    sql_text: str = single_sql["sql_text_no_comment"]

    ps = ParsedSQL(sql_text)
    this_one_sql = ps[0]
    for token in this_one_sql.tokens:
        if token.normalized == "DELETE":
            yield single_sql
            return


code_hole.append(code)

