# Author: kk.Fang(fkfkbill@gmail.com)

import abc


class BaseTicket(abc.ABC):
    """基础工单"""

    @abc.abstractmethod
    def calculate_score(self, *args, **kwargs):
        """计算工单分数"""
        pass


class BaseSubTicket(abc.ABC):
    """基础子工单"""
    pass

