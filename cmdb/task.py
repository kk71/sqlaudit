# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "BaseCMDBTask"
]

import task.const
from models.sqlalchemy import *
from task.task import BaseTask
from .cmdb_task import *
from utils.datetime_utils import *


class BaseCMDBTask(BaseTask):

    """针对纳管库的任务（定时任务，周期任务）"""

    def run(self, task_record_id: int, **kwargs):
        self.cmdb_task_id = kwargs["cmdb_task_id"]

        print(f"============"
              f"cmdb capture task({self.cmdb_task_id})"
              f"============")
        super(BaseCMDBTask, self).run(task_record_id, **kwargs)

    @classmethod
    def shoot(cls, **kwargs):
        """使用该方法启动任务而不是用delay"""

        cmdb_task_id: int = kwargs["cmdb_task_id"]
        operator: str = kwargs["operator"]

        task_record_id = cls._shoot(**kwargs)
        with make_session() as session:
            cmdb_task = session.query(CMDBTask).filter_by(id=cmdb_task_id).first()
            cmdb_task_record = CMDBTaskRecord(
                task_record_id=task_record_id,
                cmdb_task_id=cmdb_task.id,
                task_type=cls.task_type,
                task_name=task.const.ALL_TASK_TYPE_CHINESE[
                    cls.task_type],
                cmdb_id=cmdb_task.cmdb_id,
                connect_name=cmdb_task.connect_name,
                group_name=cmdb_task.group_name,
                operator=operator
            )
            session.add(cmdb_task_record)
        print(f"* going to start a cmdb task {cmdb_task_id=} with {task_record_id=} ...")
        cls.task_instance.delay(task_record_id, **kwargs)

    def on_success(self, retval, task_id, args, kwargs):
        super(BaseCMDBTask, self).on_success(retval, task_id, args, kwargs)
        with make_session() as session:
            cmdb_task = session.query(CMDBTask).filter_by(
                id=self.cmdb_task_id).first()
            cmdb_task.last_success_task_record_id = self.task_record_id
            cmdb_task.last_success_time = arrow.now().datetime
            cmdb_task.success_count += 1
            cmdb_task.exec_count += 1
            session.add(cmdb_task)

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        super(BaseCMDBTask, self).on_failure(exc, task_id, args, kwargs, einfo)
        with make_session() as session:
            cmdb_task = session.query(CMDBTask).filter_by(
                id=self.cmdb_task_id).first()
            cmdb_task.exec_count += 1
            session.add(cmdb_task)



