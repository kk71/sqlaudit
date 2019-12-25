# Author: kk.Fang(fkfkbill@gmail.com)

from utils.offline import *


class SubTicketHandler(TicketReq):

    def get(self):
        """子工单列表"""
        self.resp()

    def patch(self):
        """编辑单个子工单"""
        self.resp()


class SubTicketExportHandler(TicketReq):

    def get(self):
        """导出子工单"""
        self.resp()
