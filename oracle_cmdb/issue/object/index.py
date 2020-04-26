# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleOnlineObjectIssueIndex"
]

import rule.const
from .object import *
from issue.issue import *


class OracleOnlineObjectIssueIndex(
        OracleOnlineObjectIssue, OnlineIssueFilterWithEntriesMixin):

    ENTRIES = (rule.const.RULE_ENTRY_ONLINE_INDEX,)
