# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "CMDBTaskStats",
    "CMDBTaskStatsProcessor"
]

from mongoengine import IntField, StringField

from . import const
from models.mongoengine import *
from models.sqlalchemy import *
from .cmdb_task import *


class CMDBTaskStats(BaseDoc):
    """纳管库任务的任务状态信息"""

    task_record_id = IntField(required=True)
    cmdb_task_id = IntField(required=True)
    task_type = StringField(required=True)
    task_name = StringField(required=True)
    cmdb_id = IntField(required=True)
    connect_name = StringField(required=True)
    origin = StringField(required=True, null=True)
    stats_type = IntField(
        required=True, choices=const.ALL_CMDB_TASK_STATS_TYPE)
    data = StringField(required=True, default="")

    meta = {
        "collection": "cmdb_task_stats",
        "indexes": [
            "task_record_id",
            "cmdb_id",
            "stats_type"
        ]
    }


class CMDBTaskStatsProcessor:
    """纳管库任务状态信息的相关类，需要整合到纳管库任务，以及纳管库任务阶段的类中使用"""

    def __init__(self, task_record_id: int, **kwargs):
        self.task_record_id = task_record_id
        with make_session() as session:
            cmdb_task_record = session.query(
                CMDBTaskRecord).filter_by(
                task_record_id=task_record_id).first()
            self.keys = cmdb_task_record.to_dict(iter_if=lambda k, v: k in (
                "task_record_id",
                "cmdb_task_id",
                "task_type",
                "task_name",
                "cmdb_id",
                "connect_name"
            ))

    def write_stats(self, origin, stats_type, data=None):
        origin_name = origin.__name__
        stats = CMDBTaskStats(
            **self.keys, origin=origin_name, stats_type=stats_type, data=data)
        stats.save()
