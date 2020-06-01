# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleOnlineObjectIssueDBLink"
]

from typing import Tuple, Optional

from mongoengine import EmbeddedDocumentField, StringField

import rule.const
from ... import const
from .object import *
from ..base import OracleOnlineIssue
from issue.issue import OnlineIssueOutputParams


class OnlineIssueOutputParamsObjectDBLink(OnlineIssueOutputParams):

    dblink_name = StringField()


@OracleOnlineIssue.need_collect()
class OracleOnlineObjectIssueDBLink(OracleOnlineObjectIssue):
    """对象问题: DBLink"""

    output_params = EmbeddedDocumentField(
        OnlineIssueOutputParamsObjectDBLink,
        default=OnlineIssueOutputParamsObjectDBLink)

    ENTRIES = (rule.const.RULE_ENTRY_ONLINE_DB_LINK,)

    def get_object_unique_name(self) -> Tuple[Optional[str], str, str]:
        return self.schema_name, \
               const.ORACLE_OBJECT_TYPE_DB_LINK, \
               self.output_params.dblink_name

    def get_referred_table_name(self) -> Optional[str]:
        return None
