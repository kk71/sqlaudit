# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleOnlineObjectIssueFunction"
]

import rule.const
from .object import *
from ..base import OracleOnlineIssue


@OracleOnlineIssue.need_collect()
class OracleOnlineObjectIssueFunction(OracleOnlineObjectIssue):
    """对象问题: 函数"""

    ENTRIES = (rule.const.RULE_ENTRY_ONLINE_FUNCTION,)

