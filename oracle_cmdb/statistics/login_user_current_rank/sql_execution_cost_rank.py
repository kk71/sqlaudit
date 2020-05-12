# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleStatsCMDBSQLExecutionCostRank"
]

from typing import Generator, Union

from mongoengine import StringField, FloatField

from models.sqlalchemy import *
from .base import *
from ..base import *
from ...capture import OracleSQLStat


@OracleBaseStatistics.need_collect()
class OracleStatsCMDBSQLExecutionCostRank(
        OracleStatsMixOfLoginUserAndCurrentTaskRank):
    """登录用户与当前库的SQL执行效率排名统计"""

    BY_WHAT = ("elapsed_time_total", "elapsed_time_delta")
    LIMITATION_PER = 10

    by_what = StringField(choices=BY_WHAT)
    sql_id = StringField()
    time = FloatField()

    meta = {
        "collection": "oracle_stats_cmdb_sql_exec_cost_rank"
    }

    @classmethod
    def generate(
            cls,
            task_record_id: int,
            cmdb_id: Union[int, None],
            **kwargs) -> Generator[
                "OracleStatsCMDBSQLExecutionCostRank", None, None]:
        with make_session() as session:
            for the_user in cls.users(session, cmdb_id=cmdb_id):
                schemas = cls.schemas(
                    session,
                    cmdb_id=cmdb_id,
                    login_user=the_user.login_user
                )
                for by_what in cls.BY_WHAT:
                    stat_q = OracleSQLStat.filter(
                        task_record_id=task_record_id,
                        schema_name__in=schemas
                    ).order_by(f"-{by_what}")[:cls.LIMITATION_PER]
                    for i, stat in enumerate(stat_q):
                        doc = cls(by_what=by_what)
                        doc.sql_id = stat.sql_id
                        doc.time = getattr(stat, by_what, None)
                        cls.post_generated(
                            doc=doc,
                            task_record_id=task_record_id,
                            cmdb_id=cmdb_id,
                            target_login_user=the_user.login_user,
                            rank=i
                        )
                        yield doc
