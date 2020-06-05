# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "TaskRecord"
]

from typing import Optional

from sqlalchemy import Column, Integer, String, DateTime

from core.task import BaseTaskRecord
from models.sqlalchemy import BaseModel, ABCDeclarativeMeta
from . import const


class TaskRecord(
        BaseModel,
        BaseTaskRecord,
        metaclass=ABCDeclarativeMeta):
    """任务运行记录"""
    __tablename__ = "task_record"

    task_record_id = Column(
        "task_record_id", Integer, primary_key=True, autoincrement=True)
    task_type = Column("task_type", String)
    task_name = Column("task_name", String)
    start_time = Column("start_time", DateTime)
    end_time = Column("end_time", DateTime)
    status = Column("status", Integer, default=const.TASK_PENDING)
    operator = Column("operator", String, nullable=True)
    input = Column("input", String, nullable=True)  # 输入信息的pickle
    output = Column("output", String, nullable=True)  # 输出信息的pickle
    error_info = Column("error_info", String, default="")  # 报错信息

    @classmethod
    def last_success_task_record(cls, session, **kwargs) -> Optional["TaskRecord"]:
        task_type = kwargs["task_type"]
        return session.query(TaskRecord.task_record_id). \
            filter(cls.task_type == task_type, cls.status == const.TASK_DONE) \
            .order_by(TaskRecord.create_time.desc()).first()

