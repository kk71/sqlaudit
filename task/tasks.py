# Author: kk.Fang(fkfkbill@gmail.com)

from .task import *
from . import const


@register_task(const.TASK_TYPE_TEST)
class ATestTask(BaseTask):

    @classmethod
    def task(cls, task_record_id: int, *args, **kwargs):
        print(task_record_id)
        return "yay"
