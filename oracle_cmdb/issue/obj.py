# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleOnlineObjectIssue"
]

import rule.const
from .base import *


@OracleOnlineIssue.need_collect()
class OracleOnlineObjectIssue(OracleOnlineIssue):
    """oracle线上审核对象问题"""

    ENTRIES = (rule.const.RULE_ENTRY_ONLINE_OBJECT,)

    @classmethod
    def process(cls, collected=None, **kwargs):
        task_record_id: int = kwargs["task_record_id"]
        cmdb_id: int = kwargs["cmdb_id"]

        return
