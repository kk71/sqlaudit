# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleOnlineObjectIssueSequence"
]


import rule.const
from .object import *


class OracleOnlineObjectIssueSequence(OracleOnlineObjectIssue):

    ENTRIES = (rule.const.RULE_ENTRY_ONLINE_SEQUENCE,)

    @classmethod
    def filter(cls, *args, **kwargs):
        return cls.filter_with_inherited_entries(*args, **kwargs)
