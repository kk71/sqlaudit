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
    REQUIRES: tuple = ()

    # 收集到的子类按照互相的依赖关系排序的结果
    SORTED_COLLECTED_BY_REQUIREMENT: tuple = ()

    # 检查依赖关系时最大的循环次数
    MAX_REQUIREMENT_LOOP = 99

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

    @classmethod
    def collect(cls):
        """
        重载collect，以实现收集之后即检查依赖关系的功能
        目前按照线性执行方式，确定收集到的子类的执行顺序
        :return:
        """
        super().collect()
        collected = list(cls.COLLECTED)
        ordered_collected = []
        loop_times = 0
        while collected:
            loop_times += 1
            assert loop_times <= cls.MAX_REQUIREMENT_LOOP
            if set(collected[0].REQUIRES).issubset(ordered_collected):
                ordered_collected.append(collected[0])
                del collected[0]

        cls.SORTED_COLLECTED_BY_REQUIREMENT = tuple(ordered_collected)

