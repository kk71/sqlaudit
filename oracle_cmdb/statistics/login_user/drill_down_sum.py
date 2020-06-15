# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleStatsDashboardDrillDownSum"
]

from typing import Union, Generator, List

from mongoengine import LongField, FloatField, StringField, IntField

from models.sqlalchemy import *
from .base import *
from ...issue import *
from ..base import *


@OracleBaseStatistics.need_collect()
class OracleStatsDashboardDrillDownSum(OracleBaseTargetLoginUserStatistics):
    """登录用户所有库纳管的schema的数据和（仪表盘下钻入口）"""

    entry = StringField()
    schema_num = IntField(default=0)  # 运行采集分析的schema个数
    num = LongField(default=0)  # 采集到的[sql/object]总数（去重）
    num_with_risk = LongField(default=0)  # 包含问题的[sql/object]数（去重）
    risk_rate = FloatField(default=0.0)  # num_with_risk/num
    issue_num = LongField(default=0)  # 问题数

    meta = {
        "collection": "oracle_stats_drill_down_sum",
        "indexes": [
            "entry"
        ]
    }

    @classmethod
    def REQUIRES(cls):
        from ..login_user_target.entry_schema import OracleStatsEntrySchema
        return OracleStatsEntrySchema,

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
            **kwargs) -> Generator["OracleStatsDashboardDrillDownSum", None, None]:
        from ..login_user_target.entry_schema import OracleStatsEntrySchema

        schemas: List[str] = kwargs["schemas"]
        with make_session() as session:
            for the_user in cls.users(session):
                for entry in cls.issue_entries():
                    doc = cls(
                        entry=entry,
                        schema_num=len(schemas)
                    )
                    drill_down_qs = OracleStatsEntrySchema.filter(
                        task_record_id=task_record_id,
                        cmdb_id=cmdb_id,
                        target_login_user=the_user.login_user,
                        entry=entry
                    )
                    for drill_down_stats in drill_down_qs:
                        doc.num += drill_down_stats.num
                        doc.num_with_risk += drill_down_stats.num_with_risk
                        doc.issue_num += drill_down_stats.issue_num
                    if doc.num > 0:
                        doc.risk_rate = round(doc.num_with_risk / doc.num, 4)
                    else:
                        doc.risk_rate = 0
                    cls.post_generated(
                        doc=doc,
                        task_record_id=task_record_id,
                        cmdb_id=cmdb_id,
                        target_login_user=the_user.login_user
                    )
                    yield doc

