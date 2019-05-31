# Author: kk.Fang(fkfkbill@gmail.com)

from collections import defaultdict

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
        ret = []
        for j in jobs_to_ret:
            ret.append({
                **j.to_dict()
            })
        self.resp(ret, **p)


class OnlineReportTaskHandler(AuthReq):

    def calc_rules_and_score_in_one_result(self, session, result) -> tuple:
        # TODO 需要确定这里查看的风险规则还是全部规则
        rule_dict = rule_utils.get_rules_dict()
        score_sum = 0
        rule_name_to_detail = defaultdict(lambda: {
            "violated_num": 0,
            "rule": {},
            "deduction": 0.0,
            "weighted_deduction": 0.0
        })
        for k, rule_object in rule_dict.items():
            db_type, db_model, rule_name = k
            rule_result = getattr(result, rule_name, None)
            if rule_result:

                score_sum += rule_result["scores"]
                rule_name_to_detail[rule_name]["rule"] = rule_object.to_dict(
                    iter_if=lambda key, v: key in ("rule_name", "rule_desc"))
                rule_name_to_detail[rule_name]["violated_num"] += 1
                rule_name_to_detail[rule_name]["deduction"] += 1

        return ()

    def get(self):
        """在线查看某个报告"""
        params = self.get_query_args(Schema({
            "job_id": scm_unempty_str,
            "rule_type": scm_one_of_choices(rule_utils.ALL_RULE_TYPE)
        }))
        job_id = params.pop("job_id")
        rule_type = params.pop("rule_type")
        del params  # shouldn't use params anymore
        with make_session() as session:
            job = Job.objects(_id=job_id).first()
            result = Results.objects(task_uuid=job_id).first()
            cmdb = session.query(CMDB).filter_by(cmdb_id=result.cmdb_id)
            rules_violated = []
            self.resp({
                "job_id": job_id,
                "cmdb": cmdb.to_dict(),
                "rules_violated": rules_violated,
                "rule_type": rule_type,
                "schema": job.desc["owner"]
            })


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


