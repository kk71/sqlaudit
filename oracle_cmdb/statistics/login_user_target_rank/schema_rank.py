# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleStatsSchemaRank"
]

from typing import Generator, Union, List

from mongoengine import FloatField

from models.sqlalchemy import *
from ..base import OracleBaseStatistics
from .base import OracleStatsMixOfLoginUserAndTargetSchemaRank
from ..current_task import OracleStatsSchemaScore


@OracleBaseStatistics.need_collect()
class OracleStatsSchemaRank(OracleStatsMixOfLoginUserAndTargetSchemaRank):
    """登录用户全部纳管库的schema排名"""

    score = FloatField()

    meta = {
        "collection": "oracle_stats_schema_rank"
    }

    REQUIRES = (OracleStatsSchemaScore,)

    @classmethod
    def generate(
            cls,
            task_record_id: int,
            cmdb_id: Union[int, None],
            **kwargs) -> Generator["OracleStatsSchemaRank", None, None]:
        with make_session() as session:
            for the_user in cls.users(session):

                # 每个登录用户产生一批记录，所以要按照用户分组
                latris: List[int] = []
                for the_cmdb in cls.cmdbs(session, login_user=the_user.login_user):
                    latri = cls.cmdb_latest_available_task_record_id_for_stats(
                        cmdb_id=cmdb_id,
                        task_record_id=task_record_id,
                        target_cmdb=the_cmdb
                    )
                    if latri and latri not in latris:
                        latris.append(latri)

                schema_rate_q = OracleStatsSchemaScore.filter(
                    task_record_id__in=latris).limit(10)
                for i, a_schema_rate in enumerate(schema_rate_q):
                    doc = cls(
                        score=a_schema_rate.schema_score()
                    )
                    cls.post_generated(
                        doc=doc,
                        cmdb_id=cmdb_id,
                        task_record_id=task_record_id,
                        target_login_user=the_user.login_user,
                        target_cmdb_id=a_schema_rate.cmdb_id,
                        target_task_record_id=a_schema_rate.task_record_id,
                        target_schema_name=a_schema_rate.schema_name,
                        rank=i
                    )
                    yield doc
