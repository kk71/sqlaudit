# Author: kk.Fang(fkfkbill@gmail.com)

from schema import Optional, Schema, And

from utils.schema_utils import *
from utils import rule_utils
from restful_api.views.base import AuthReq
from models.mongo import *
from models.oracle import *


class OnlineReportTaskListHandler(AuthReq):

    def get(self):
        """在线查看报告任务列表"""
        params = self.get_query_args(Schema({
            Optional("cmdb_id"): scm_int,
            Optional("schema_name", default=None): scm_unempty_str,
            "status": And(scm_int, scm_one_of_choices((0, 1, 2))),
            Optional("date_start", default=None): scm_datetime,
            Optional("date_end", default=None): scm_datetime,

            Optional("page", default=1): scm_int,
            Optional("per_page", default=10): scm_int,
        }))
        p = self.pop_p(params)
        schema_name = params.pop("schema_name")
        date_start, date_end = params.pop("date_start"), params.pop("date_end")
        job_q = Job.objects(**params)
        if schema_name:
            job_q = job_q.filter(desc__owner=schema_name)
        if date_start:
            job_q = job_q.filter(create_time__gte=date_start)
        if date_end:
            job_q = job_q.filter(create_time__lte=date_end)
        jobs_to_ret, p = self.paginate(job_q, **p)
        with make_session() as session:
            cmdb_dict = {i.cmdb_id: i.to_dict() for i in session.query(CMDB).all()}
            ret = []
            for j in jobs_to_ret:
                ret.append({
                    "cmdb": cmdb_dict[j.cmdb_id],
                    **j.to_dict()
                })
            self.resp(ret, **p)


class OnlineReportTaskHandler(AuthReq):

    def get(self):
        """在线查看某个报告"""
        self.resp()


class OnlineReportRuleDetailHandler(AuthReq):

    def get(self):
        """在线查看报告的规则细节(obj返回一个列表，其余返回sql文本相关的几个列表)"""
        self.resp()


class OnlineReportSQLPlanHandler(AuthReq):

    def get(self):
        """在线查看报告sql执行计划信息（仅适用于非obj）"""
        self.resp()


class ExportReportXlsxHandler(AuthReq):

    def get(self):
        """导出报告为xlsx"""
        self.resp()


class ExportReportHTMLHandler(AuthReq):

    def get(self):
        """导出报告为html"""
        self.resp()


