# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "BaseStatisticItem"
]

import abc
from typing import Union, NoReturn

from .self_collecting_class import *


class BaseStatisticItem(SelfCollectingFramework):
    """基础统计对象"""

    cmdb_id = None  # 纳管库id
    task_record_id = None  # 任务id
    create_time = None  # 统计时间

    # 依赖关系
    REQUIRES = []

    @classmethod
    @abc.abstractmethod
    def generate(
            cls,
            task_record_id: int,
            cmdb_id: Union[int, None],
            **kwargs) -> NoReturn:
        """
        产生统计数据
        :param task_record_id: 调度当前统计的任务id
        :param cmdb_id: 如果cmdb_id为None，则表示是不针对任何一个库的统计
        """
        pass

    @classmethod
    def check_requires(cls):
        """检查依赖关系"""
        pass

    # @classmethod
    # def collect(cls):
    #     """
    #     重载collect，以实现收集之后即检查依赖关系的功能
    #     检查完依赖关系，会构建一个依赖关系树，方便以后做分布式
    #     :return:
    #     """
    #     super().collect()
    #
    #     return

