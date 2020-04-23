# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "values_dict",
    "recursively_find_following_select"
]

"""

专门给规则代码使用的工具类

"""

from sqlparse.sql import TokenList

from models.mongoengine import *

values_dict = values_dict


def recursively_find_following_select(token: TokenList) -> bool:
    if token.normalized == "SELECT":
        return True
    if isinstance(token, TokenList) and token.tokens:
        for new_token in token.tokens:
            recursive_ret = recursively_find_following_select(new_token)
            if recursive_ret:
                return True
    return False


