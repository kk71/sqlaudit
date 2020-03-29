from sqlparse.sql import TokenList

from utils.parsed_sql import ParsedSQL


def recursively_find_following_select(token: TokenList) -> bool:
    print(token.normalized)
    if token.normalized == "SELECT":
        return True
    if isinstance(token, TokenList) and token.tokens:
        for new_token in token.tokens:
            recursive_ret = recursively_find_following_select(new_token)
            if recursive_ret:
                return True
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
