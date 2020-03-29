from sqlparse.sql import TokenList

from utils.parsed_sql import ParsedSQL


def recursively_find_following_select(token: TokenList) -> bool:
    if not isinstance(token, TokenList):
        return False
    if token.normalized == "SELECT":
        return True
    if token.tokens:
        for new_token in token.tokens:
            return recursively_find_following_select(new_token)
    return False


def code(rule, entries, **kwargs):
    single_sql: dict = kwargs.get("single_sql")
    sql_text: str = single_sql["sql_text_no_comment"]
    ps = ParsedSQL(sql_text)
    this_one_sql = ps[0]
    has_from = False
    for token in this_one_sql.tokens:
        if token.normalized == "FROM":
            has_from = True
            continue
        if has_from:
            if recursively_find_following_select(token):
                return -rule.weight, []
    return None, []


code_hole.append(code)
