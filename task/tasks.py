# Author: kk.Fang(fkfkbill@gmail.com)

from .task import *
from . import const


@register_task(const.TASK_TYPE_TEST)
class TestTask(BaseTask):
    """测试任务"""

    @classmethod
    def task(cls, task_record_id: int, **kwargs):
        return 1
