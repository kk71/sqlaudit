# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "CMDBTask",
    "CMDBTaskRecord",
    "BaseCMDBTask"
]

from typing import Dict, Union, Optional, Tuple

from redis import StrictRedis
from sqlalchemy import Column, Integer, String, Boolean, DateTime, or_, and_

import settings
import task.const
from task.task import BaseTask
from utils.datetime_utils import *
from models.sqlalchemy import *
from task.task_record import TaskRecord


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
    schedule_time = Column("schedule_time", String, default="22:00", nullable=True)
    frequency = Column("frequency", Integer, default=60*60*24, nullable=True)  # 单位分钟
    exec_count = Column("exec_count", Integer, default=0)
    success_count = Column("success_count", Integer, default=0)
    # 最后一次任务id（无论什么状态
    last_task_record_id = Column("last_task_record_id", Integer, nullable=True)
    # 最后一次成功的任务记录id
    last_success_task_record_id = Column("last_success_task_record_id", nullable=True)
    last_success_time = Column("last_success_time", DateTime, nullable=True)

    def flush_celery_q(self):
        redis_celery_broker = StrictRedis(
            host=settings.REDIS_BROKER_IP,
            port=settings.REDIS_BROKER_PORT,
            db=settings.REDIS_BROKER_DB
        )
        redis_celery_broker.ltrim(self.task_type, 1, 0)

    def day_last_succeed_task_record_id(
            self,
            date_start: Union[date, arrow.Arrow],
            date_end: Union[date, arrow.Arrow],
            task_record_id_supposed_to_be_succeed: Optional[int] = None
            ) -> Dict[date, int]:
        """
        获取某个日期区间内每一天的最后一次成功的task_record_id字典
        :param date_start:
        :param date_end:
        :param task_record_id_supposed_to_be_succeed:
                对于某个正在进行的任务，需要将它也包括到成功的任务中。
                但是这个task_record_id必须是当前任务的
        :return:
        """
        session = self._sa_instance_state.session
        # 这里以任务开始时间作为判断采集到的数据的时间
        # 因为sql的采集都是根据snapshotid去判断，开始时间更接近获取snap_shot_id的时间
        # 实际肯定有误差，这个误差暂时就不管了，极限情况的发生概率很低
        q = TaskRecord.status == task.const.TASK_DONE
        # 确保输入的是日期（而非日期时间）
        if isinstance(date_start, datetime):
            date_start = date_start.date()
        if isinstance(date_end, datetime):
            date_end = date_end.date()
        # 如果不是arrow，则转换为arrow
        if not isinstance(date_start, arrow.Arrow):
            date_start = arrow.get(date_start)
        if not isinstance(date_end, arrow.Arrow):
            date_end = arrow.get(date_end)
        # 过滤日期
        q = and_(q, TaskRecord.start_time >= date_start.date())
        q = and_(q, TaskRecord.start_time <= date_end.shift(days=+1).date())
        # 考虑task_record_id_supposed_to_be_succeed
        if task_record_id_supposed_to_be_succeed:
            q = or_(
                q,
                TaskRecord.task_record_id ==
                task_record_id_supposed_to_be_succeed
            )
        ret = {
            # 为了保证即使那日没有成功运行的任务，也不至于key里面没有日期
            d.date(): None
            for d in arrow.Arrow.range("day", date_start, date_end)
        }
        qs = session.query(*(qe := QueryEntity(
            TaskRecord.start_time,
            CMDBTaskRecord.task_record_id
        ))).filter(q).order_by(
            TaskRecord.start_time)  # 排序很关键
        for dt, task_record_id in qs:
            ret[dt.date()] = task_record_id
        return ret

    @classmethod
    def query_cmdb_task_with_last_record(
            cls,
            session) -> Tuple[sqlalchemy_q, QueryEntity]:
        """查询纳管库任务，以及各自最后一次任务记录"""
        ret = session.query(*(qe := QueryEntity(
            CMDBTask.id,
            CMDBTask.task_type,
            CMDBTask.task_name,
            CMDBTask.cmdb_id,
            CMDBTask.connect_name,
            CMDBTask.group_name,
            CMDBTask.status,
            CMDBTask.schedule_time,
            CMDBTask.frequency,
            CMDBTask.exec_count,
            CMDBTask.success_count,
            CMDBTask.last_success_time,
            TaskRecord.status.label("execution_status"),
            TaskRecord.error_info,
            CMDBTaskRecord.operator
        ))).filter(
            CMDBTask.id == CMDBTaskRecord.cmdb_task_id,
            CMDBTask.last_task_record_id == TaskRecord.task_record_id
        )
        return ret, qe


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

    @classmethod
    def query_cmdb_task_record_with_task_record(
            cls,
            session) -> Tuple[sqlalchemy_q, QueryEntity]:
        """联合查询纳管库任务和任务纪录表"""
        ret = session.query(*(qe := QueryEntity(
            CMDBTaskRecord.task_record_id,
            CMDBTaskRecord.cmdb_task_id,
            CMDBTaskRecord.task_type,
            CMDBTaskRecord.task_name,
            CMDBTaskRecord.cmdb_id,
            CMDBTaskRecord.connect_name,
            CMDBTaskRecord.group_name,
            CMDBTaskRecord.operator,
            TaskRecord.start_time,
            TaskRecord.end_time,
            TaskRecord.status,
            TaskRecord.error_info,
            TaskRecord.input,
            TaskRecord.output
        ))).filter(
            CMDBTaskRecord.task_record_id == TaskRecord.task_record_id
        ).first()
        return ret, qe


class BaseCMDBTask(BaseTask):
    """针对纳管库的任务（定时任务，周期任务）"""

    def run(self, task_record_id: int, **kwargs):
        self.cmdb_task_id = kwargs["cmdb_task_id"]

        print(f"============"
              f"cmdb capture task({self.cmdb_task_id})"
              f"============")
        super(BaseCMDBTask, self).run(task_record_id, **kwargs)

    @classmethod
    def shoot(cls, **kwargs) -> int:
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
                operator=operator,
            )
            session.add(cmdb_task_record)
            cmdb_task.exec_count += 1
            cmdb_task.last_task_record_id = task_record_id
            session.add(cmdb_task)
        print(f"* going to start a cmdb task {cmdb_task_id=} with {task_record_id=} ...")
        cls.task_instance.delay(task_record_id, **kwargs)
        return task_record_id

    def on_success(self, retval, task_id, args, kwargs):
        super(BaseCMDBTask, self).on_success(retval, task_id, args, kwargs)
        with make_session() as session:
            cmdb_task = session.query(CMDBTask).filter_by(
                id=self.cmdb_task_id).first()
            cmdb_task.last_success_task_record_id = self.task_record_id
            cmdb_task.last_success_time = arrow.now().datetime
            cmdb_task.success_count += 1
            session.add(cmdb_task)
