# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "Task",
    "TaskRecord",
]

from sqlalchemy import Column, String, Integer, DateTime, Boolean

from core.task import *
from models.sqlalchemy import *


class Task(BaseModel):
    """任务"""
    __tablename__ = "task"

    id = Column("id", Integer, primary_key=True)
    task_type = Column("task_type", String)
    task_name = Column("task_name", String)
    cmdb_id = Column("cmdb_id", Integer)
    connect_name = Column("connect_name", String)
    group_name = Column("group_name", String)
    db_type = Column("db_type", String)
    status = Column("status", Boolean, default=True)
    schedule_time = Column("schedule_time", String, default="22:00")
    frequency = Column("frequency", Integer, default=60*60*24)  # 单位分钟
    exec_count = Column("exec_count", Integer, default=0)
    exec_success_count = Column("exec_success_count", Integer, default=0)
    last_exec_success_time = Column("last_exec_success_time", DateTime)

    @classmethod
    def filter_execution_status(cls, execution_status: bool = None) -> sqlalchemy_q:
        """过滤特定运行状态的任务"""
        pass


class TaskRecord(
        BaseModel,
        BaseTaskRecord,
        metaclass=ABCDeclarativeMeta):
    """任务运行记录"""
    __tablename__ = "task_record"

    task_record_id = Column("task_record_id", Integer, primary_key=True)
    task_id = Column("task_id", Integer)
    task_type = Column("task_type", String)
    task_name = Column("task_name", String)
    cmdb_id = Column("cmdb_id", Integer)
    connect_name = Column("connect_name", String)
    start_time = Column("start_time", DateTime)
    end_time = Column("end_time", DateTime)
    status = Column("status", Integer)
    operator = Column("operator", String)

