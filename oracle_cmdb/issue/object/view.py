# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleOnlineObjectIssueView"
]

import rule.const
from .object import *
from ..base import OracleOnlineIssue


@OracleOnlineIssue.need_collect()
class OracleOnlineObjectIssueView(OracleOnlineObjectIssue):
    """对象问题: 视图"""

    ENTRIES = (rule.const.RULE_ENTRY_ONLINE_VIEW,)

