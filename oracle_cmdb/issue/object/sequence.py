# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleOnlineObjectIssueSequence"
]


import rule.const
from .object import *
from issue.issue import *


class OracleOnlineObjectIssueSequence(
        OracleOnlineObjectIssue, OnlineIssueFilterWithEntriesMixin):

    ENTRIES = (rule.const.RULE_ENTRY_ONLINE_SEQUENCE,)
