# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleOnlineObjectIssueIndex"
]

import rule.const
from .object import *


class OracleOnlineObjectIssueIndex(OracleOnlineObjectIssue):

    ENTRIES = (rule.const.RULE_ENTRY_ONLINE_INDEX,)

    @classmethod
    def filter(cls, *args, **kwargs):
        return cls.filter_with_entries(*args, **kwargs)
