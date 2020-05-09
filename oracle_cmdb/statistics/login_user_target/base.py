# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleStatsMixOfLoginUserAndTargetSchema"
]

from typing import Generator

from ...cmdb import *
from ..login_user.base import *
from ..target.base import *
from ...auth.user_utils import *


class OracleStatsMixOfLoginUserAndTargetSchema(
        OracleBaseTargetLoginUserStatistics,
        OracleBaseTargetSchemaStatistics):

    meta = {
        "abstract": True
    }

    @classmethod
    def post_generated(cls, **kwargs):
        OracleBaseTargetLoginUserStatistics.post_generated(**kwargs)
        OracleBaseTargetSchemaStatistics.post_generated(**kwargs)

    @classmethod
    def cmdbs(cls, session, **kwargs) -> Generator[OracleCMDB, None, None]:
        login_user: str = kwargs["login_user"]
        cmdb_ids = current_cmdb(session, login_user)
        for a_cmdb in OracleBaseTargetSchemaStatistics.cmdbs(session):
            if a_cmdb.cmdb_id in cmdb_ids:
                yield a_cmdb

