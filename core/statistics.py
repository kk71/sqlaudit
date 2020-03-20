# Author: kk.Fang(fkfkbill@gmail.com)

import abc
from typing import Union


class BaseStatisticItem(abc.ABC):
    """基础统计对象"""

    cmdb_id = None  # 纳管库id
    task_record_id = None  # 任务id
    create_time = None  # 统计时间

    # 依赖关系检查
    requires = ()

    @classmethod
    @abc.abstractmethod
    def generate(cls, task_record_id: int, cmdb_id: Union[int, None]):
        """
        产生统计数据
        :param task_record_id: 调度当前统计的任务id
        :param cmdb_id: 如果cmdb_id为None，则表示是不针对任何一个库的统计
        """
        pass


class BaseStatistic(abc.ABC):
    """基础统计"""

    # 统计对象
    statistic_items: (BaseStatisticItem,) = ()

    def run(self):
        """启动统计"""
        return
