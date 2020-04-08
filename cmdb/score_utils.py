
import arrow

from cmdb.cmdb import CMDB
from utils import score_utils
from models.sqlalchemy import make_session,QueryEntity


def get_latest_cmdb_score(collect_month=1) -> dict:
    """
    查询纳管库最近一次评分信息
    :param session:
    :param collect_month: 评分时限(月内)
    :return: {cmdb_id: StatsCMDBRate, ...}
    """
    with make_session() as session:
        task_record_ids = list(score_utils.get_latest_task_record_id().values())
        all_cmdb_ids = QueryEntity.to_plain_list(session.query(CMDB.cmdb_id))
        q = StatsCMDBRate.objects(
            etl_date__gte=arrow.now().shift(months=-collect_month).datetime,
            task_record_id__in=task_record_ids
        )
        cmdb_id_stats_cmdb_rate_pairs = {i.cmdb_id: i for i in q}
        return {
            cmdb_id: cmdb_id_stats_cmdb_rate_pairs.get(cmdb_id, StatsCMDBRate(cmdb_id=cmdb_id))
            for cmdb_id in all_cmdb_ids  # 保证一定能取到
        }