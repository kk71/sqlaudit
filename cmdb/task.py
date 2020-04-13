# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "BaseCMDBTask"
]

import task.const
from models.sqlalchemy import *
from task.task import BaseTask
from .cmdb_task import *


class BaseCMDBTask(BaseTask):

    """针对纳管库的任务（定时任务，周期任务）"""

    @classmethod
    def shoot(cls, **kwargs):
        task_id: int = kwargs["task_id"]
        operator: str = kwargs["operator"]

        task_record_id = cls._shoot(**kwargs)
        with make_session() as session:
            cmdb_task = session.query(CMDBTask).filter_by(task_id=task_id).first()
            cmdb_task_record = CMDBTaskRecord(
                task_record_id=task_record_id,
                cmdb_task_id=cmdb_task.id,
                task_type=cls.task_type,
                task_name=task.const.ALL_TASK_EXECUTION_STATUS_CHINESE_MAPPING[
                    cls.task_type],
                cmdb_id=cmdb_task.cmdb_id,
                connect_name=cmdb_task.connect_name,
                group_name=cmdb_task.group_name,
                operator=operator
            )
            session.add(cmdb_task_record)
        cls.task_instance.delay(task_record_id, **kwargs)
