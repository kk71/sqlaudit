# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "BaseTaskRecord",
    "BaseStage"
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


class BaseStage(abc.ABC):
    """基础任务的阶段"""

    task_record_id = None  # 任务记录id
    cmdb_id = None  # 纳管库id
    stage_id = None  # 阶段id
    required_stage_id = None  # 依赖的阶段id
    start_time = None  # 开始时间
    end_time = None  # 开始时间
    status = None  # 状态

    @abc.abstractmethod
    def run(self):
        """执行当前阶段"""
        pass
