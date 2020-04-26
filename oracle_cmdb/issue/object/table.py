# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleOnlineObjectIssueTable"
]


import rule.const
from .object import *
from issue.issue import *


class OracleOnlineObjectIssueTable(
        OracleOnlineObjectIssue, OnlineIssueFilterWithEntriesMixin):

    ENTRIES = (rule.const.RULE_ENTRY_ONLINE_TABLE,)
