# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleOnlineObjectIssueFunction"
]

from typing import Tuple, Optional

from mongoengine import StringField, EmbeddedDocumentField

import rule.const
from ... import const
from .object import *
from ..base import OracleOnlineIssue
from issue.issue import OnlineIssueOutputParams


class OnlineIssueOutputParamsObjectFunction(OnlineIssueOutputParams):

    func_name = StringField()


@OracleOnlineIssue.need_collect()
class OracleOnlineObjectIssueFunction(OracleOnlineObjectIssue):
    """对象问题: 函数"""

    output_params = EmbeddedDocumentField(
        OnlineIssueOutputParamsObjectFunction,
        default=OnlineIssueOutputParamsObjectFunction)

    ENTRIES = (rule.const.RULE_ENTRY_ONLINE_FUNCTION,)

    def get_object_unique_name(self) -> Tuple[Optional[str], str, str]:
        return self.schema_name, \
               const.ORACLE_OBJECT_TYPE_FUNCTION, \
               self.output_params.func_name
