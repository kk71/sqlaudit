# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleStatsMixOfLoginUserAndCurrentCMDBRank"
]

from ..rank import OracleStatsRank
from ..login_user_current import OracleStatsMixOfLoginUserAndCurrentCMDB


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

