# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "BaseTask",
    "register_task"
]

import pickle
import traceback

from kombu import Exchange, Queue

from utils.datetime_utils import *
from models.sqlalchemy import *
from . import celery_conf
from .celery import celery_app
from .task_record import *


def add_task(task_type: str, module_to_import: str):
    """增加任务"""
    celery_conf.task_routes.update({
        task_type: {'queue': task_type, 'routing_key': task_type}
    })
    celery_conf.task_queues.add(
        Queue(task_type, Exchange(task_type), routing_key=task_type)
    )
    celery_conf.imports.append(module_to_import)


def register_task(task_type: str):
    """注册任务"""
    def inner(task_class):
        task_class.name = task_type
        task_class.task_type = task_type
        add_task(task_type, task_class.__module__)
        celery_app.register_task(task_class())
        task_instance = celery_app.tasks[task_class.name]
        task_class.task_instance = task_instance
        return task_instance

    return inner


class BaseTask(celery_app.Task):
    """基础任务"""

    # 任务类型
    name = task_type = None
    # name字段是celery预留的，task_type是项目字段
    # name用于celery标识任务的名称，本质就是task_type

    def run(self, task_record_id: int, **kwargs):
        self.task_record_id = task_record_id
        print(f"going to run {self.task_type}: {self.task_record_id} ...")
        with make_session() as session:
            task_record = session.query(TaskRecord). \
                filter_by(task_record_id=self.task_record_id).first()
            task_record.status = const.TASK_RUNNING
            session.add(task_record)
        return self.task(task_record_id, **kwargs)

    @classmethod
    def task(cls, task_record_id: int, **kwargs):
        """
        重写该方法以实现任务的实际功能
        TODO 该方法写成静态，是为了和celery脱离关系，
            使得必要时可以静态运行任务而不需要队列
        :param task_record_id:
        :param kwargs:
        :return:
        """
        raise NotImplementedError

    def on_success(self, retval, task_id, args, kwargs):
        with make_session() as session:
            task_record = session.query(TaskRecord). \
                filter_by(task_record_id=self.task_record_id).first()
            task_record.status = const.TASK_DONE
            task_record.end_time = arrow.now().datetime
            task_record.output = pickle.dumps(retval, protocol=0).decode()
            session.add(task_record)

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        failure_info = traceback.format_exc()
        print(f"task({self.task_record_id}) just failed: \n\n{failure_info}\n")
        with make_session() as session:
            task_record = session.query(TaskRecord). \
                filter_by(task_record_id=self.task_record_id).first()
            task_record.status = const.TASK_FAILED
            task_record.end_time = arrow.now().datetime
            task_record.error_info = failure_info[-4999:]
            session.add(task_record)

    @classmethod
    def _shoot(cls, **kwargs) -> int:
        with make_session() as session:
            task_record = TaskRecord(
                task_type=cls.task_type,
                task_name=const.ALL_TASK_TYPE_CHINESE[cls.task_type],
                start_time=arrow.now().datetime,
                input=pickle.dumps(kwargs, protocol=0).decode()
            )
            session.add(task_record)
            session.commit()
            return task_record.task_record_id

    @classmethod
    def shoot(cls, **kwargs):
        task_record_id = cls._shoot(**kwargs)
        cls.task_instance.delay(task_record_id, **kwargs)
