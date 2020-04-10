# Author: kk.Fang(fkfkbill@gmail.com)

from .task import *


@register_task("emm")
class Emm(BaseTask):

    # 任务类型
    name = task_type = None

    def run(self, task_record_id: int=None, **kwargs):
        print("run")
        assert 0

    def on_success(self, retval, task_id, args, kwargs):
        print("success")
        # with make_session() as session:
        #     task_record = session.query(TaskRecord). \
        #         filter_by(task_record_id=task_record_id).first()
        #     task_record.status = const.TASK_DONE
        #     task_record.end_time = arrow.now().datetime
        #     session.add(task_record)

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        print("failure")
        # with make_session() as session:
        #     task_record = session.query(TaskRecord). \
        #         filter_by(task_record_id=task_record_id).first()
        #     task_record.status = const.TASK_FAILED
        #     task_record.end_time = arrow.now().datetime
        #     task_record.error_info = traceback.format_exc()
        #     session.add(task_record)

    @classmethod
    def shoot(cls, **kwargs):
        # with make_session() as session:
        #     task_record = TaskRecord(
        #         task_type=self.task_type,
        #         task_name=const.ALL_TASK_TYPE_CHINESE[task_type],
        #         start_time=arrow.now().datetime
        #     )
        #     session.add(task_record)
        #     session.commit()
        #     task_record_id = task_record.task_record_id
        #     cls.delay(task_record_id, **kwargs)
        pass

