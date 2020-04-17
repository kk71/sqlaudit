# Author: kk.Fang(fkfkbill@gmail.com)

from .base import *


@BaseOracleOnlineIssue.need_collect()
class OracleOnlineSQLIssue(BaseOracleOnlineIssue):
    """oracle线上审核sql问题"""

    pass
