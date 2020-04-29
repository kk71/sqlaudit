# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "CMDBTaskStats"
]

from mongoengine import IntField, StringField

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

    meta = {
        "collection": "cmdb_task_stats",
        "allow_inheritance": True,
        "indexes": [
            "task_record_id",
            "cmdb_id"
        ]
    }

    @classmethod
    def write_stats(cls, task_record_id: int, origin, **kwargs):
        origin_name = origin.__name__
        with make_session() as session:
            cmdb_task_record = session.query(
                CMDBTaskRecord).filter_by(
                task_record_id=task_record_id).first()
            keys = cmdb_task_record.to_dict(iter_if=lambda k, v: k in (
                "task_record_id",
                "cmdb_task_id",
                "task_type",
                "task_name",
                "cmdb_id",
                "connect_name"
            ))
        stats = CMDBTaskStats(
            **keys,
            origin=origin_name,
            **kwargs
        )
        stats.save()
