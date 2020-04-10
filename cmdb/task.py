# Author: kk.Fang(fkfkbill@gmail.com)

from sqlalchemy import Column, Integer, String, Boolean, DateTime

from models.sqlalchemy import BaseModel


class CMDBTask(BaseModel):
    """纳管库任务"""
    __tablename__ = "cmdb_task"

    id = Column("id", Integer, primary_key=True)
    task_type = Column("task_type", String)
    task_name = Column("task_name", String)
    cmdb_id = Column("cmdb_id", Integer)
    connect_name = Column("connect_name", String)
    group_name = Column("group_name", String)
    db_type = Column("db_type", String)
    status = Column("status", Boolean, default=True)  # 启用或者禁用
    schedule_time = Column("schedule_time", String, default="22:00")
    frequency = Column("frequency", Integer, default=60*60*24)  # 单位分钟
    exec_count = Column("exec_count", Integer, default=0)
    exec_success_count = Column("exec_success_count", Integer, default=0)
    last_exec_success_time = Column("last_exec_success_time", DateTime)
