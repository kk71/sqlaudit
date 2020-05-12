# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleStatsMixOfLoginUserAndCurrentCMDB"
]

from typing import Generator

from ...cmdb import *
from auth.user import User
from ...auth.user_utils import current_cmdb
from ..login_user import OracleBaseTargetLoginUserStatistics
from ..current_cmdb import *


class OracleStatsMixOfLoginUserAndCurrentCMDB(
        OracleBaseTargetLoginUserStatistics,
        OracleBaseCurrentCMDBStatistics):
    """登录用户与当前纳管库的统计"""

    meta = {
        "abstract": True
    }

    @classmethod
    def post_generated(cls, **kwargs):
        OracleBaseTargetLoginUserStatistics.post_generated(**kwargs)
        OracleBaseCurrentCMDBStatistics.post_generated(**kwargs)

    @classmethod
    def users(cls, session, **kwargs) -> Generator[User, None, None]:
        """只yield绑定了当前任务的纳管库的用户"""
        cmdb_id: int = kwargs["cmdb_id"]
        for the_user in super().users(session):
            current_cmdb_id: [int] = current_cmdb(
                session, login_user=the_user.login_user)
            if cmdb_id in current_cmdb_id:
                yield the_user

    @classmethod
    def cmdbs(cls, session, **kwargs) -> Generator[OracleCMDB, None, None]:
        cmdb_id: int = kwargs["cmdb_id"]
        for the_cmdb in super().cmdbs(session):
            if the_cmdb.cmdb_id == cmdb_id:
                yield the_cmdb

