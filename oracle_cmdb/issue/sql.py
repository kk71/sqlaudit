# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleOnlineSQLIssue",
    "OracleOnlineIssueOutputParamsSQL"
]

from mongoengine import StringField, EmbeddedDocumentField

from issue.issue import OnlineIssueOutputParams
import rule.const
from .base import *


class OracleOnlineIssueOutputParamsSQL(OnlineIssueOutputParams):
    """针对SQL的输出字段"""

    sql_id = StringField(required=True, default=None)

    meta = {
        "allow_inheritance": True
    }


class OracleOnlineSQLIssue(OracleOnlineIssue):
    """sql问题"""

    output_params = EmbeddedDocumentField(OracleOnlineIssueOutputParamsSQL)

    meta = {
        "allow_inheritance": True
    }

    ENTRIES = (rule.const.RULE_ENTRY_ONLINE_SQL,)

