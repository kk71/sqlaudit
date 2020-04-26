# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleOnlineObjectIssueTable"
]


import rule.const
from .object import *


class OracleOnlineObjectIssueTable(OracleOnlineObjectIssue):

    ENTRIES = (rule.const.RULE_ENTRY_ONLINE_TABLE,)
