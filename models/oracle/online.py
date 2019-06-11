# Author: kk.Fang(fkfkbill@gmail.com)

from sqlalchemy import Column, String, Integer, Boolean, Sequence
from sqlalchemy.dialects.oracle import DATE, CLOB

from .utils import BaseModel


class DataHealth(BaseModel):
    __tablename__ = "T_DATA_HEALTH"

    database_name = Column("DATABASE_NAME", String)
    health_score = Column("HEALTH_SCORE", Integer)
    collect_date = Column("COLLECT_DATE", DATE, primary_key=True)


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
    task_status = Column("TASK_STATUS", Boolean)
    task_schedule_date = Column("TASK_SCHEDULE_DATE", String)
    task_exec_scripts = Column("TASK_EXEC_SCRIPTS", String)
    task_exec_counts = Column("TASK_EXEC_COUNTS", Integer)
    task_exec_success_count = Column("TASK_EXEC_SUCCESS_COUNTS", Integer, default=0)
    last_task_exec_succ_date = Column("LAST_TASK_EXEC_SUCC_DATE", DATE)
    task_create_date = Column("TASK_CREATE_DATE", DATE)
    cmdb_id = Column("CMDB_ID", Integer)
    task_exec_frequency = Column("TASK_EXEC_FREQUENCY", Integer)


class TaskExecHistory(BaseModel):
    """任务执行历史"""
    __tablename__ = "T_TASK_EXEC_HISTORY"
    id = Column("ID", Sequence("SEQ_TASK_EXEC_HISTORY"), primary_key=True)
    task_id = Column("TASK_ID", Integer)
    connect_name = Column("CONNECT_NAME", String)
    business_name = Column("BUSINESS_NAME", String)
    task_start_date = Column("TASK_START_DATE", DATE)
    task_end_date = Column("TASK_END_DATE", DATE)
    status = Column("STATUS", Boolean)
    error_msg = Column("ERROR_MSG", String)
    task_uuid = Column("TASK_UUID", String)
