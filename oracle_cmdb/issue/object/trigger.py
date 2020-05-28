# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleOnlineObjectIssueTrigger"
]

import rule.const
from .object import *
from ..base import OracleOnlineIssue


@OracleOnlineIssue.need_collect()
class OracleOnlineObjectIssueTrigger(OracleOnlineObjectIssue):
    """对象问题: trigger"""

    ENTRIES = (rule.const.RULE_ENTRY_ONLINE_TRIGGER,)

