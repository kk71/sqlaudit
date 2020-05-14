# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "latest_cmdb_score"
]

from typing import Dict, Any

from models.sqlalchemy import *
from .cmdb import *
from .statistics import OracleStatsCMDBScore


def latest_cmdb_score(session) -> Dict[str, Dict[str, Any]]:
    """
    查询纳管库最近一次评分信息
    """
    all_cmdb_ids = QueryEntity.to_plain_list(session.query(OracleCMDB.cmdb_id))
    scores = OracleStatsCMDBScore.latest_cmdb_score()
    return {
        # 能够确保如果某个库没有数据，那么该库的字段不会不存在
        cmdb_id: scores.get(cmdb_id, {"cmdb_score": None, "create_time": None})
        for cmdb_id in all_cmdb_ids
    }
