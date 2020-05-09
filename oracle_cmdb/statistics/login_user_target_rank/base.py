# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleStatsMixOfLoginUserAndTargetCMDBRank",
    "OracleStatsMixOfLoginUserAndTargetSchemaRank"
]

from ..login_user_target.base import *
from ..rank.base import *


class OracleStatsMixOfLoginUserAndTargetCMDBRank(
        OracleStatsMixOfLoginUserAndTargetCMDB,
        OracleStatsRank):

    meta = {
        "abstract": True
    }

    @classmethod
    def post_generated(cls, **kwargs):
        OracleStatsMixOfLoginUserAndTargetCMDB.post_generated(**kwargs)
        OracleStatsRank.post_generated(**kwargs)


class OracleStatsMixOfLoginUserAndTargetSchemaRank(
        OracleStatsMixOfLoginUserAndTargetSchema,
        OracleStatsRank):

    meta = {
        "abstract": True
    }

    @classmethod
    def post_generated(cls, **kwargs):
        OracleStatsMixOfLoginUserAndTargetSchema.post_generated(**kwargs)
        OracleStatsRank.post_generated(**kwargs)

