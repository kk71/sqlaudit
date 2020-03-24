# Author: kk.Fang(fkfkbill@gmail.com)

import abc

from .base import *
from restful_api.views.base import *


class OnlineOverviewHandler(PrivilegeReq, abc.ABC):

    @abc.abstractmethod
    def get(self):
        """上线情况概览"""
        pass


class OnlineExecuteHandler(TicketReq, abc.ABC):

    @abc.abstractmethod
    def post(self):
        """执行上线"""
        pass
