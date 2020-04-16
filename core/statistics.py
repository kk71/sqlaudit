# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "BaseStatisticItem"
]

import abc
from typing import Union


class BaseStatisticItem(abc.ABC):
    """基础统计对象"""

    cmdb_id = None  # 纳管库id
    task_record_id = None  # 任务id
    create_time = None  # 统计时间

    # 要统计的模块
    MODELS = []

    # 依赖关系
    REQUIRES = []

    @classmethod
    @abc.abstractmethod
    def generate(cls, task_record_id: int, cmdb_id: Union[int, None]):
        """
        产生统计数据
        :param task_record_id: 调度当前统计的任务id
        :param cmdb_id: 如果cmdb_id为None，则表示是不针对任何一个库的统计
        """
        pass

    @classmethod
    def need_stats(cls):
        """装饰需要采集的model"""
        def inner(model):
            assert issubclass(model, cls)
            cls.MODELS.append(model)
            return model
        return inner

    @classmethod
    @abc.abstractmethod
    def check_requires(cls):
        """检查依赖关系"""
        pass

    @abc.abstractmethod
    def run(self):
        """启动统计"""
        return
