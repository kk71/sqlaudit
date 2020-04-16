# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "BaseTaskRecord"
]

import abc


class BaseTaskRecord(abc.ABC):
    """基础任务执行记录"""

    task_record_id = None  # 任务记录id
    task_type = None  # 任务类型(这是一个有限的枚举)
    cmdb_id = None  # 纳管库id
    start_time = None  # 开始时间
    end_time = None  # 开始时间
    status = None  # 状态
    operator = None  # 任务操作人
