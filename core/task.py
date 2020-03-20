# Author: kk.Fang(fkfkbill@gmail.com)

import abc


class BaseTask(abc.ABC):
    """基础任务"""

    @abc.abstractmethod
    def run(self):
        """执行任务"""
        pass


class BaseStage(abc.ABC):
    """基础任务的阶段"""
    pass
