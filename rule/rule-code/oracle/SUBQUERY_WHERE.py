from parsed_sql.parsed_sql import ParsedSQL

from rule.code_utils import *


def code(rule, entries, **kwargs):
    single_sql: dict = kwargs.get("single_sql")
    sql_text: str = single_sql["sql_text_no_comment"]

    ps = ParsedSQL(sql_text)
    this_one_sql = ps[0]
    has_from = False
    for token in this_one_sql.tokens:
        if token.normalized == "WHERE":
            has_from = True
            continue
        if has_from:
            if recursively_find_following_select(token):
                yield single_sql


code_hole.append(code)
