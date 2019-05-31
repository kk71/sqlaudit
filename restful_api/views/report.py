# Author: kk.Fang(fkfkbill@gmail.com)

from collections import defaultdict

from schema import Optional, Schema, And

from utils.schema_utils import *
from utils import rule_utils, cmdb_utils, result_utils
from restful_api.views.base import AuthReq
from models.mongo import *
from models.oracle import *


class OnlineReportTaskListHandler(AuthReq):

    def get(self):
        """在线查看报告任务列表"""
        params = self.get_query_args(Schema({
            Optional("cmdb_id"): scm_int,
            Optional("schema_name", default=None): scm_unempty_str,
            "status": And(scm_int, scm_one_of_choices(result_utils.ALL_JOB_STATUS)),
            Optional("date_start", default=None): scm_date,
            Optional("date_end", default=None): scm_date,

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

    @staticmethod
    def calc_deduction(scores):
        return round(float(scores) / 1.0, 2)

    @staticmethod
    def calc_weighted_deduction(scores, total_score):
        return round(float(scores) * 100 / (total_score or 1), 2)

    @classmethod
    def calc_rules_and_score_in_one_result(cls, result, cmdb) -> tuple:
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
                if result.rule_type == rule_utils.RULE_TYPE_OBJ:
                    rule_name_to_detail[rule_name]["violated_num"] += \
                        len(rule_result.get("records", []))

                if result.rule_type in [
                    rule_utils.RULE_TYPE_TEXT,
                    rule_utils.RULE_TYPE_SQLSTAT,
                    rule_utils.RULE_TYPE_SQLPLAN]:
                    rule_name_to_detail[rule_name]["violated_num"] += \
                        len(rule_result.get("sqls", []))
                else:
                    assert 0

                score_sum += float(rule_result["scores"])
                rule_name_to_detail[rule_name]["rule"] = rule_object.to_dict(
                    iter_if=lambda key, v: key in ("rule_name", "rule_desc"))
                rule_name_to_detail[rule_name]["deduction"] += \
                    cls.calc_deduction(rule_result["scores"])
                rule_name_to_detail[rule_name]["weighted_deduction"] += \
                    cls.calc_weighted_deduction(
                        rule_result["scores"],
                        rule_utils.calc_sum_of_rule_max_score(
                            db_type=cmdb_utils.DB_ORACLE,
                            db_model=cmdb.db_model,
                            rule_type=result.rule_type
                        )
                    )

        return list(rule_name_to_detail.values()), score_sum

    def get(self):
        """在线查看某个报告"""
        params = self.get_query_args(Schema({
            "job_id": scm_unempty_str,
        }))
        job_id = params.pop("job_id")
        del params  # shouldn't use params anymore
        with make_session() as session:
            job = Job.objects(id=job_id).first()
            result = Results.objects(task_uuid=job_id).first()
            cmdb = session.query(CMDB).filter_by(cmdb_id=result.cmdb_id).first()
            if not cmdb:
                self.resp_not_found(msg="纳管数据库不存在")
                return
            rules_violated, score_sum = self.calc_rules_and_score_in_one_result(result, cmdb)
            self.resp({
                "job_id": job_id,
                "cmdb": cmdb.to_dict(),
                "rules_violated": rules_violated,
                "score_sum": score_sum,
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


