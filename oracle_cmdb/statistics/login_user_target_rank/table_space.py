# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleStatsTableSpaceRank"
]

from typing import Union, Generator, List

from mongoengine import StringField, FloatField

from models.sqlalchemy import *
from ..base import OracleBaseStatistics
from .base import OracleStatsMixOfLoginUserAndTargetCMDBRank
from ...capture import OracleObjTabSpace


@OracleBaseStatistics.need_collect()
class OracleStatsTableSpaceRank(OracleStatsMixOfLoginUserAndTargetCMDBRank):
    """用户的纳管库表空间排名"""

    tablespace_name = StringField()
    usage_ratio = FloatField()

    meta = {
        "collection": "oracle_stats_cmdb_rank"
    }

    @classmethod
    def generate(
            cls,
            task_record_id: int,
            cmdb_id: Union[int, None],
            **kwargs) -> Generator["OracleStatsTableSpaceRank", None, None]:
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

                tab_space_q = OracleObjTabSpace.filter(
                    task_record_id__in=latris).order_by("-usage_ratio")
                for i, a_tab_space in enumerate(tab_space_q):
                    doc = cls(
                        **a_tab_space.to_dict(
                            iter_if=lambda k, v: k in (
                                "tablespace_name", "usage_ratio")
                        )
                    )
                    cls.post_generated(
                        doc=doc,
                        cmdb_id=cmdb_id,
                        task_record_id=task_record_id,
                        target_login_user=the_user.login_user,
                        target_cmdb_id=a_tab_space.cmdb_id,
                        target_task_record_id=a_tab_space.task_record_id,
                        rank=i
                    )
                    yield doc
