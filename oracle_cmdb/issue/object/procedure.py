# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleOnlineObjectIssueProcedure"
]

import rule.const
from .object import *
from ..base import OracleOnlineIssue


@OracleOnlineIssue.need_collect()
class OracleOnlineObjectIssueProcedure(OracleOnlineObjectIssue):
    """对象问题: 存储过程"""

    ENTRIES = (rule.const.RULE_ENTRY_ONLINE_PROCEDURE,)

