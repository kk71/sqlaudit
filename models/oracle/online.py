# Author: kk.Fang(fkfkbill@gmail.com)

from sqlalchemy import Column, String, Integer, Boolean, Sequence
from sqlalchemy.dialects.oracle import DATE, CLOB

from .utils import BaseModel
from utils.datetime_utils import *


class DataHealth(BaseModel):
    __tablename__ = "T_DATA_HEALTH"

    id = Column("ID", String, Sequence("SEQ_DATA_HEALTH"), primary_key=True)
    database_name = Column("DATABASE_NAME", String)
    health_score = Column("HEALTH_SCORE", Integer)
    collect_date = Column("COLLECT_DATE", DATE)


class TaskManage(BaseModel):
    __tablename__ = "T_TASK_MANAGE"

    task_id = Column("TASK_ID", Integer, Sequence("SEQ_TASK_MANAGE"), primary_key=True)
    connect_name = Column("CONNECT_NAME", String)
    group_name = Column("GROUP_NAME", String)
    business_name = Column("BUSINESS_NAME", String)
    machine_room = Column("MACHINE_ROOM", Integer)
    database_type = Column("DATABASE_TYPE", Integer)
    server_name = Column("SERVER_NAME", String)
    ip_address = Column("IP_ADDRESS", String)
    port = Column("PORT", Integer)
    task_status = Column("TASK_STATUS", Boolean, default=True)
    task_schedule_date = Column("TASK_SCHEDULE_DATE", String, default="22:00")
    task_exec_scripts = Column("TASK_EXEC_SCRIPTS", String)
    task_exec_counts = Column("TASK_EXEC_COUNTS", Integer, default=0)
    task_exec_success_count = Column("TASK_EXEC_SUCCESS_COUNTS", Integer, default=0)
    last_task_exec_succ_date = Column("LAST_TASK_EXEC_SUCC_DATE", DATE)
    task_create_date = Column("TASK_CREATE_DATE", DATE, default=datetime.now)
    cmdb_id = Column("CMDB_ID", Integer)
    task_exec_frequency = Column("TASK_EXEC_FREQUENCY", Integer, default=60*60*24)  # 单位分钟


class TaskExecHistory(BaseModel):
    """任务执行历史"""
    __tablename__ = "T_TASK_EXEC_HISTORY"

    id = Column("ID", Integer, Sequence("SEQ_TASK_EXEC_HISTORY"), primary_key=True)
    task_id = Column("TASK_ID", Integer)
    connect_name = Column("CONNECT_NAME", String)
    business_name = Column("BUSINESS_NAME", String)
    task_start_date = Column("TASK_START_DATE", DATE, default=datetime.now)
    task_end_date = Column("TASK_END_DATE", DATE, nullable=True)
    status = Column("STATUS", Boolean, nullable=True)
    error_msg = Column("ERROR_MSG", String, nullable=True)
    operator = Column("OPERATOR", String, nullable=True)  # null自动采集 str采集发起人的login_user

    @classmethod
    def filter_succeed(cls, session, *args, **kwargs):
        return session.query(cls).filter(*args, cls.status == True, **kwargs)
