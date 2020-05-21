# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleStatsEntryCMDB"
]

from typing import Union, Generator

from mongoengine import LongField, FloatField, ListField, StringField

from models.sqlalchemy import *
from .base import *
from ..base import *


@OracleBaseStatistics.need_collect()
class OracleStatsEntryCMDB(OracleStatsMixOfLoginUserAndTargetCMDB):
    """登录用户各库各维度对象数问题数和风险率"""

    connect_name = StringField(default="")
    entry = StringField()
    entries = ListField()
    num = LongField(default=0)  # 采集到的[sql/object]总数（去重）
    num_with_risk = LongField(default=0)  # 包含问题的[sql/object]数（去重）
    risk_rate = FloatField(default=0)  # num_with_risk/num
    issue_num = LongField(default=0)  # 问题数

    meta = {
        "collection": "oracle_stats_entry_cmdb",
        "indexes": [
            "entry",
            "entries"
        ]
    }

    @classmethod
    def REQUIRES(cls):
        from .entry_schema import OracleStatsEntrySchema
        cls.ISSUES = OracleStatsEntrySchema.ISSUES
        return OracleStatsEntrySchema,

    @classmethod
    def generate(
            cls,
            task_record_id: int,
            cmdb_id: Union[int, None],
            **kwargs) -> Generator["OracleStatsEntryCMDB", None, None]:
        from .entry_schema import OracleStatsEntrySchema

        with make_session() as session:
            for the_user in cls.users(session):
                for the_cmdb in cls.cmdbs(session, login_user=the_user.login_user):
                    for issue_model in cls.ISSUES:
                        entry_schema_q = OracleStatsEntrySchema.filter(
                            task_record_id=task_record_id,
                            target_login_user=the_user.login_user,
                            target_cmdb_id=the_cmdb.cmdb_id,
                            entry=issue_model.ENTRIES[0]
                        )
                        doc = cls(
                            connect_name=the_cmdb.connect_name,
                            entry=issue_model.ENTRIES[0],
                            entries=issue_model.INHERITED_ENTRIES
                        )
                        for entry_schema_stats in entry_schema_q:
                            doc.num += entry_schema_stats.num
                            doc.num_with_risk += entry_schema_stats.num_with_risk
                            doc.issue_num += entry_schema_stats.issue_num
                        if doc.num:
                            doc.risk_rate = round(doc.num_with_risk / doc.num, 4)
                        target_task_record_id = entry_schema_stats.target_task_record_id
                        cls.post_generated(
                            doc=doc,
                            task_record_id=task_record_id,
                            cmdb_id=cmdb_id,
                            target_login_user=the_user.login_user,
                            target_cmdb_id=the_cmdb.cmdb_id,
                            target_task_record_id=target_task_record_id
                        )
                        yield doc

