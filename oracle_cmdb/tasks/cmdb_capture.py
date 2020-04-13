# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleCMDBCaptureTask"
]

import task.const
from cmdb.task import *
from task.task import *


@register_task(task.const.TASK_TYPE_CAPTURE)
class OracleCMDBCaptureTask(BaseCMDBTask):

    """纳管库采集（包括采集、分析、统计三步骤）"""

    @classmethod
    def task(cls, task_record_id: int, **kwargs):
        print("in task!")
