# Author: kk.Fang(fkfkbill@gmail.com)


from .base import AuthReq


class RiskListHandler(AuthReq):

    def get(self):
        """风险列表"""
        self.resp()


class RiskReportExportHandler(AuthReq):

    def get(self):
        """导出风险报告"""
        self.resp()


class RiskDetailHandler(AuthReq):

    def get(self):
        """风险详情（include sql text, sql plan and statistics）"""
        self.resp()


class SQLPlanHandler(AuthReq):

    def get(self):
        """风险详情的sql plan详情"""
        self.resp()

