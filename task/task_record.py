# Author: kk.Fang(fkfkbill@gmail.com)

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
    cmdb_id = Column("cmdb_id", Integer)
    connect_name = Column("connect_name", String)
    start_time = Column("start_time", DateTime)
    end_time = Column("end_time", DateTime)
    status = Column("status", Integer, default=const.TASK_PENDING)
    operator = Column("operator", String)
    meta_info = Column("meta_info", String)  # 附加信息
    error_info = Column("error_info", String)  # 报错信息
