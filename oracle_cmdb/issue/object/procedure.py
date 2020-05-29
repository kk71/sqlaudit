# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleOnlineObjectIssueProcedure"
]

from typing import Tuple, Optional

from mongoengine import StringField, EmbeddedDocumentField

import rule.const
from .object import *
from ... import const
from ..base import OracleOnlineIssue
from issue.issue import OnlineIssueOutputParams


class OnlineIssueOutputParamsObjectProcedure(OnlineIssueOutputParams):

    proc_name = StringField(null=True)


@OracleOnlineIssue.need_collect()
class OracleOnlineObjectIssueProcedure(OracleOnlineObjectIssue):
    """对象问题: 存储过程"""

    output_params = EmbeddedDocumentField(
        OnlineIssueOutputParamsObjectProcedure,
        default=OnlineIssueOutputParamsObjectProcedure)

    ENTRIES = (rule.const.RULE_ENTRY_ONLINE_PROCEDURE,)

    def get_object_unique_name(self) -> Tuple[Optional[str], str, str]:
        return self.schema_name, \
               const.ORACLE_OBJECT_TYPE_PROCEDURE, \
               self.output_params.proc_name
