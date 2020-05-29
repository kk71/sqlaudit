# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleOnlineObjectIssueTrigger"
]

from typing import Optional, Tuple

from mongoengine import StringField, EmbeddedDocumentField

import rule.const
from ... import const
from .object import *
from ..base import OracleOnlineIssue
from issue.issue import OnlineIssueOutputParams


class OnlineIssueOutputParamsObjectTrigger(OnlineIssueOutputParams):

    trigger_name = StringField(null=True)


@OracleOnlineIssue.need_collect()
class OracleOnlineObjectIssueTrigger(OracleOnlineObjectIssue):
    """对象问题: trigger"""

    output_params = EmbeddedDocumentField(
        OnlineIssueOutputParamsObjectTrigger,
        default=OnlineIssueOutputParamsObjectTrigger)

    ENTRIES = (rule.const.RULE_ENTRY_ONLINE_TRIGGER,)

    def get_object_unique_name(self) -> Tuple[Optional[str], str, str]:
        return self.schema_name, \
               const.ORACLE_OBJECT_TYPE_TRIGGER, \
               self.output_params.trigger_name

