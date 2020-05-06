# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "CMDBTask",
    "CMDBTaskRecord"
]

from sqlalchemy import Column, Integer, String, Boolean, DateTime

from models.sqlalchemy import BaseModel
from task.const import ALL_TASK_EXECUTION_STATUS


class CMDBTask(BaseModel):
    """纳管库任务"""
    __tablename__ = "cmdb_task"

    id = Column("id", Integer, primary_key=True, autoincrement=True)
    task_type = Column("task_type", String)
    task_name = Column("task_name", String)
    cmdb_id = Column("cmdb_id", Integer)
    connect_name = Column("connect_name", String)
    group_name = Column("group_name", String)
    db_type = Column("db_type", String)
    status = Column("status", Boolean, default=True)  # 启用或者禁用
    execution_status = Column("execution_status",Integer,default=ALL_TASK_EXECUTION_STATUS[0])
    schedule_time = Column("schedule_time", String, default="22:00", nullable=True)
    frequency = Column("frequency", Integer, default=60*60*24, nullable=True)  # 单位分钟
    exec_count = Column("exec_count", Integer, default=0)
    success_count = Column("success_count", Integer, default=0)
    last_success_task_record_id = Column("last_success_task_record_id", nullable=True)
    last_success_time = Column("last_success_time", DateTime, nullable=True)


class CMDBTaskRecord(BaseModel):
    """纳管库的任务记录"""
    __tablename__ = "cmdb_task_record"

    task_record_id = Column("task_record_id", Integer, primary_key=True)
    cmdb_task_id = Column("cmdb_task_id", Integer)
    task_type = Column("task_type", String)
    task_name = Column("task_name", String)
    cmdb_id = Column("cmdb_id", Integer)
    connect_name = Column("connect_name", String)
    group_name = Column("group_name", String)
    # 操作来源：定时任务，频率任务，页面发起(记录是的login_user)，命令行发起
    operator = Column("operator", String)

