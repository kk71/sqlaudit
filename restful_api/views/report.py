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
    def calc_weighted_deduction(scores, max_score_sum):
        return round(float(scores) * 100 / (max_score_sum or 1), 2)

    @classmethod
    def calc_rules_and_score_in_one_result(cls, result, cmdb) -> tuple:
        score_sum_of_all_rule_scores_in_result = 0
        max_score_sum = rule_utils.calc_sum_of_rule_max_score(
            db_type=cmdb_utils.DB_ORACLE,
            db_model=cmdb.db_model,
            rule_type=result.rule_type
        )
        rule_name_to_detail = defaultdict(lambda: {
            "violated_num": 0,
            "rule": {},
            "deduction": 0.0,
            "weighted_deduction": 0.0
        })

        for rule_object in Rule.objects(rule_type=result.rule_type,
                                        db_model=cmdb.db_model,
                                        db_type=cmdb_utils.DB_ORACLE):
            rule_result = getattr(result, rule_object.rule_name, None)
            if rule_result:
                if result.rule_type == rule_utils.RULE_TYPE_OBJ:
                    rule_name_to_detail[rule_object.rule_name]["violated_num"] += \
                        len(rule_result.get("records", []))

                if result.rule_type in [
                    rule_utils.RULE_TYPE_TEXT,
                    rule_utils.RULE_TYPE_SQLSTAT,
                    rule_utils.RULE_TYPE_SQLPLAN]:
                    rule_name_to_detail[rule_object.rule_name]["violated_num"] += \
                        len(rule_result.get("sqls", []))
                else:
                    assert 0

                score_sum_of_all_rule_scores_in_result += round(float(rule_result["scores"]) / 1.0, 2)
                rule_name_to_detail[rule_object.rule_name]["rule"] = rule_object.to_dict(
                    iter_if=lambda key, v: key in ("rule_name", "rule_desc"))
                rule_name_to_detail[rule_object.rule_name]["deduction"] += \
                    cls.calc_deduction(rule_result["scores"])
                rule_name_to_detail[rule_object.rule_name]["weighted_deduction"] += \
                    cls.calc_weighted_deduction(
                        rule_result["scores"],
                        max_score_sum=max_score_sum
                    )
        scores_total = round((max_score_sum - score_sum_of_all_rule_scores_in_result) / max_score_sum * 100 or 1, 2)
        scores_total = scores_total if scores_total > 40 else 40

        return list(rule_name_to_detail.values()), scores_total

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

    @staticmethod
    def get_report_rule_detail(session, job_id: int, rule_name: str) -> dict:
        job = Job.objects(id=job_id).first()
        cmdb = session.query(CMDB).filter_by(cmdb_id=job.cmdb_id).first()
        rule = Rule.objects(
            rule_name=rule_name,
            rule_type=job.desc.rule_type,
            db_model=cmdb.db_model
        ).first()
        result = Results.objects(task_uuid=job_id).first()
        rule_dict_in_rst = getattr(result, rule_name)
        records = []
        columns = []

        if rule.rule_type == rule_utils.RULE_TYPE_OBJ:
            columns = [i["parm_desc"] for i in rule.output_parms]
            for r in rule_dict_in_rst.get("records", []):
                if data not in records:
                    records.append(dict(zip(columns, r)))

        elif rule.rule_type in [rule_utils.RULE_TYPE_SQLPLAN,
                                rule_utils.RULE_TYPE_SQLSTAT]:
            for sql_dict in rule_dict_in_rst["sqls"]:
                if sql_dict.get("obj_name", None):
                    obj_name = sql_dict["obj_name"]
                else:
                    obj_name = "空"
                if sql_dict.get("cost", None):
                    cost = sql_dict["cost"]
                else:
                    cost = "空"
                if sql_dict.get("stat", None):
                    count = sql_dict["stat"].get("ts_cnt", "空")
                else:
                    count = "空"
                records.append({
                    "sql_id": sql_dict["sql_id"],
                    "sql_text": sql_dict["sql_text"],
                    "plan_hash_value": sql_dict["plan_hash_value"],
                    "pos": "v",
                    "object_name": obj_name,
                    "cost": cost,
                    "count": count
                })
            if records:
                columns = list(records[0].keys())

        elif rule.rule_type == rule_utils.RULE_TYPE_TEXT:
            records = [{
                "sql_id": i["sql_id"],
                "sql_text": i["sql_text"]
            } for i in rule_dict_in_rst["sqls"]]
            if records:
                columns = list(records[0].keys())

        return {
            "columns": columns,
            "records": records,
            "rule": rule
        }

    def get(self):
        """在线查看报告的规则细节(obj返回一个列表，其余类型返回sql文本相关的几个列表)"""
        params = self.get_query_args(Schema({
            "job_id": scm_unempty_str,
            "rule_name": scm_unempty_str
        }))
        job_id = params.pop("job_id")
        rule_name = params.pop("rule_name")
        del params

        with make_session() as session:
            ret = self.get_report_rule_detail(session, job_id, rule_name)
            self.resp({
                "columns": ret["columns"],
                "records": ret["records"],
                "rule": ret["rule"].to_dict(iter_if=lambda k, v: k in (
                    "rule_desc", "rule_name", "rule_type", "solution")),
            })


class OnlineReportSQLPlanHandler(AuthReq):

    def get(self):
        """在线查看报告sql执行计划信息（仅适用于非obj）"""
        params = self.get_query_args(Schema({
            "sql_id": scm_unempty_str,
            "job_id": scm_unempty_str,
            "rule_name": scm_unempty_str
        }))
        sql_id = params.pop("sql_id")
        job_id = params.pop("job_id")
        rule_name = params.pop("rule_name")
        del params

        sql = SQLText.objects(sql_id=sql_id).first()
        result = Results.objects(task_uuid=job_id).first()
        rule_dict_in_rst = getattr(result, rule_name, {})
        execution_stat = {}
        plan_hash_value = 0
        plans = []
        if result.rule_type in (rule_utils.RULE_TYPE_SQLPLAN, rule_utils.RULE_TYPE_SQLSTAT):
            for sql_dict in rule_dict_in_rst.get("sqls", []):
                if sql_dict and sql_dict["sql_id"] == sql_id:
                    execution_stat = sql_dict["stat"]
                    plan_hash_value = sql_dict["plan_hash_value"]
                    break
        if plan_hash_value:
            plans = [i.to_dict(iter_if=lambda k, v:k in (
                "operation",
                "options",
                "object_owner",
                "object_name",
                "cost",
                "cardinality"
            )) for i in MSQLPlan.objects(plan_hash_value=plan_hash_value, sql_id=sql_id).
                order_by("-etl_date")]

        self.resp({
            "sql_text": sql.sql_text,
            "sql_plan": plans,
            "execution_stat": execution_stat
        })


class ExportReportXLSXHandler(AuthReq):

    def get(self):
        """导出报告为xlsx"""
        self.resp()


class ExportReportHTMLHandler(AuthReq):

    def get(self):
        """导出报告为html"""
        self.resp()
