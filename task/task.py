# Author: kk.Fang(fkfkbill@gmail.com)

from sqlalchemy import Column, String, Integer, DateTime

from core.task import *
from new_models.sqlalchemy import *


class TaskRecord(BaseModel, BaseTaskRecord):
    """任务运行记录"""

    task_record_id = Column(Integer, primary_key=True)
    task_type = Column(Integer)
    cmdb_id = Column(Integer)
    start_time = Column(DateTime)
    end_time = Column(DateTime)
    status = Column(Integer)
    operator = Column(String)

    def run(self):
        """执行任务"""
        pass
