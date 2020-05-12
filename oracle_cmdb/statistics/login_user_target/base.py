# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleStatsMixOfLoginUserAndTargetCMDB",
    "OracleStatsMixOfLoginUserAndTargetSchema"
]

from typing import Generator

from ...cmdb import *
from ..login_user.base import *
from ..target.base import *
from ...auth.user_utils import *


class OracleStatsMixOfLoginUserAndTargetCMDB(
        OracleBaseTargetLoginUserStatistics,
        OracleBaseTargetCMDBStatistics):
    """登录用户绑定的纳管库统计"""

    meta = {
        "abstract": True
    }

    @classmethod
    def post_generated(cls, **kwargs):
        OracleBaseTargetLoginUserStatistics.post_generated(**kwargs)
        OracleBaseTargetCMDBStatistics.post_generated(**kwargs)

    @classmethod
    def cmdbs(cls, session, **kwargs) -> Generator[OracleCMDB, None, None]:
        login_user: str = kwargs["login_user"]
        cmdb_ids = current_cmdb(session, login_user)
        for a_cmdb in OracleBaseTargetCMDBStatistics.cmdbs(session):
            if a_cmdb.cmdb_id in cmdb_ids:
                yield a_cmdb


class OracleStatsMixOfLoginUserAndTargetSchema(
        OracleStatsMixOfLoginUserAndTargetCMDB,
        OracleBaseTargetSchemaStatistics):
    """登录用户绑定的纳管库schema统计"""

    meta = {
        "abstract": True
    }

    @classmethod
    def post_generated(cls, **kwargs):
        OracleStatsMixOfLoginUserAndTargetCMDB.post_generated(**kwargs)
        OracleBaseTargetSchemaStatistics.post_generated(**kwargs)
