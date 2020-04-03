# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "TaskRecord",
    "TaskManage",
    "TaskExecHistory"
]

from sqlalchemy import Column, String, Integer, DateTime, Boolean
from sqlalchemy import Sequence

from core.task import *
from models.sqlalchemy import *
from utils.datetime_utils import *


class TaskRecord(BaseModel, BaseTaskRecord):
    """任务运行记录"""
    __tablename__ = "task_record"

    task_record_id = Column("task_record_id", Integer, primary_key=True)
    task_type = Column("task_type", Integer)
    cmdb_id = Column("cmdb_id", Integer)
    start_time = Column("start_time", DateTime)
    end_time = Column("end_time", DateTime)
    status = Column("status", Integer)
    operator = Column("operator", String)

    def run(self):
        """执行任务"""
        pass


class TaskManage(BaseModel):
    __tablename__ = "task_manage"

    task_id = Column("task_id", Integer, primary_key=True)
    cmdb_id = Column("cmdb_id", Integer)
    connect_name = Column("connect_name", String)
    group_name = Column("group_name", String)
    business_name = Column("business_name", String)
    machine_room = Column("machine_room", Integer)
    database_type = Column("database_type", Integer)
    server_name = Column("server_name", String)
    ip_address = Column("ip_address", String)
    port = Column("port", Integer)
    task_status = Column("task_status", Boolean, default=True)
    task_schedule_date = Column("task_schedule_date", String, default="22:00")
    task_exec_scripts = Column("task_exec_scripts", String)
    task_exec_counts = Column("task_exec_counts", Integer, default=0)
    task_exec_success_count = Column("task_exec_success_count", Integer, default=0)
    last_task_exec_succ_date = Column("last_task_exec_succ_date", DateTime)
    task_create_date = Column("task_create_date", DateTime, default=datetime.now)
    task_exec_frequency = Column("task_exec_frequency", Integer, default=60*60*24)  # 单位分钟


class TaskExecHistory(BaseModel):
    """任务执行历史"""
    __tablename__ = "task_exec_history"

    id = Column("id", Integer, primary_key=True)
    task_id = Column("task_id", Integer)
    connect_name = Column("connect_name", String)
    business_name = Column("business_name", String)
    task_start_date = Column("task_start_date", DateTime, default=datetime.now)
    task_end_date = Column("task_end_date", DateTime, nullable=True)
    status = Column("status", Boolean, nullable=True)
    error_msg = Column("error_msg", String, nullable=True)
    operator = Column("operator", String, nullable=True)  # null自动采集 str采集发起人的login_user

    @classmethod
    def filter_succeed(cls, session, *args, **kwargs):
        return session.query(cls).filter(*args, cls.status == True, **kwargs)
