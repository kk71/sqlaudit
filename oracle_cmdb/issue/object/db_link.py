# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleOnlineObjectIssueDBLink"
]

import rule.const
from .object import *
from ..base import OracleOnlineIssue


@OracleOnlineIssue.need_collect()
class OracleOnlineObjectIssueDBLink(OracleOnlineObjectIssue):
    """对象问题: DBLink"""

    ENTRIES = (rule.const.RULE_ENTRY_ONLINE_DB_LINK,)

