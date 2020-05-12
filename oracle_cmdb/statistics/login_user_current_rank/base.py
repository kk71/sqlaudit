# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleStatsMixOfLoginUserAndCurrentCMDBRank",
    "OracleStatsMixOfLoginUserAndCurrentTaskRank"
]

from ..rank import OracleStatsRank
from ..login_user_current import *


class OracleStatsMixOfLoginUserAndCurrentCMDBRank(
        OracleStatsMixOfLoginUserAndCurrentCMDB,
        OracleStatsRank):

    meta = {
        "abstract": True
    }

    @classmethod
    def post_generated(cls, **kwargs):
        OracleStatsMixOfLoginUserAndCurrentCMDB.post_generated(**kwargs)
        OracleStatsRank.post_generated(**kwargs)


class OracleStatsMixOfLoginUserAndCurrentTaskRank(
        OracleStatsMixOfLoginUserAndCurrentTask,
        OracleStatsRank):

    meta = {
        "abstract": True
    }

    @classmethod
    def post_generated(cls, **kwargs):
        OracleStatsMixOfLoginUserAndCurrentTask.post_generated(**kwargs)
        OracleStatsRank.post_generated(**kwargs)

