# Author: kk.Fang(fkfkbill@gmail.com)

from schema import Schema

from models.oracle import *
from models.mongo import *
from utils.const import *
from utils.schema_utils import *
from utils.datetime_utils import *
from utils.offline_utils import *


class TicketOuterHandler(OfflineTicketCommonHandler):

    def get(self):
        """线下工单的外层归档列表，按照日期，工单类型，审核结果来归档"""
        self.resp()


class TicketHandler(OfflineTicketCommonHandler):

    def get(self):
        """工单列表"""
        self.resp()

    def post(self):
        """提交工单"""
        self.resp()

    def patch(self):
        """编辑工单状态"""
        self.resp()

    def delete(self):
        """删除工单"""
        self.resp()


class TicketExportHandler(OfflineTicketCommonHandler):

    def get(self):
        """导出工单"""
        self.resp()


class SubTicketHandler(OfflineTicketCommonHandler):

    def get(self):
        """子工单列表"""
        self.resp()

    def patch(self):
        """编辑单个子工单"""
        self.resp()


class SubTicketExportHandler(OfflineTicketCommonHandler):

    def get(self):
        """导出子工单"""
        self.resp()
