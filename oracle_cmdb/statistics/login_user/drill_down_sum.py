# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleStatsDashboardDrillDownSum"
]

from typing import Union, Generator

from mongoengine import LongField, FloatField

from models.sqlalchemy import *
from .base import *
from ...issue import *
from ..login_user_target import OracleStatsDashboardDrillDown


@OracleBaseStatistics.need_collect()
class OracleStatsDashboardDrillDownSum(OracleBaseTargetLoginUserStatistics):
    """仪表盘下钻数据的和（即：下钻入口）"""

    entry = StringField()
    num = LongField(default=0)  # 采集到的[sql/object]总数（去重）
    num_with_risk = LongField(default=0)  # 包含问题的[sql/object]数（去重）
    num_with_risk_rate = FloatField(default=0.0)  # problem_num/num
    issue_num = LongField(default=0)  # 问题数

    meta = {
        "collection": "oracle_stats_drill_down_sum",
        "indexes": [
            "entry"
        ]
    }

    REQUIRES = (OracleStatsDashboardDrillDown,)

    ISSUES = (
        OracleOnlineSQLIssue,
        OracleOnlineObjectIssueTable,
        OracleOnlineObjectIssueSequence,
        OracleOnlineObjectIssueIndex
    )

    @classmethod
    def generate(
            cls,
            task_record_id: int,
            cmdb_id: Union[int, None],
            **kwargs) -> Generator["OracleStatsDashboardDrillDown", None, None]:
        with make_session() as session:
            for the_user in cls.users(session):
                for entry in cls.issue_entries():
                    doc = cls(entry=entry)
                    drill_down_qs = OracleStatsDashboardDrillDown.filter(
                        task_record_id=task_record_id,
                        cmdb_id=cmdb_id,
                        target_login_user=the_user.login_user,
                        entries=entry
                    )
                    for drill_down_stats in drill_down_qs:
                        doc.num += drill_down_stats.num
                        doc.num_with_risk += drill_down_stats.num_with_risk
                        doc.issue_num += drill_down_stats.issue_num
                    doc.num_with_risk_rate = round(doc.num_with_risk / doc.num, 4)
                    cls.post_generated(
                        doc=doc,
                        task_record_id=task_record_id,
                        cmdb_id=cmdb_id,
                        target_login_user=the_user.login_user
                    )
                    yield doc

