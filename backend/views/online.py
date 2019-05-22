# Author: kk.Fang(fkfkbill@gmail.com)

from collections import defaultdict

from schema import Schema, Optional
from mongoengine import Q

from .base import AuthReq
from backend.utils.schema_utils import *
from backend.utils import rule_utils, cmdb_utils
from backend.models.mongo import Rule, Results
from backend.models.oracle import *


class RiskListHandler(AuthReq):

    def get(self):
        """风险列表"""
        params = self.get_query_args(Schema({
            Optional("type", default="ALL"): scm_one_of_choices(["ALL"] +
                                                                list(rule_utils.ALL_RULE_TYPE)),
            "cmdb_id": scm_int,
            Optional("schema_name", default=None): scm_str,
            Optional("risk_sql_rule_id", default=None): scm_dot_split_int,
            Optional("date_start", default=None): scm_date,
            Optional("date_end", default=None): scm_date,

            Optional("page", default=1): scm_int,
            Optional("per_page", default=10): scm_int,

            # for risk sqls
            Optional(object): object
        }))
        rule_type = params.pop("type")
        cmdb_id = params.pop("cmdb_id")
        schema_name = params.pop("schema_name")
        risk_sql_rule_id_list = params.pop("risk_sql_rule_id")
        date_start, date_end = params.pop("date_start"), params.pop("date_end")
        p = self.pop_p(params)

        with make_session() as session:
            cmdb = session.query(CMDB).filter_by(cmdb_id=cmdb_id).first()
            if not cmdb:
                self.resp_bad_req(msg=f"不存在编号为{cmdb_id}的cmdb。")
                return
            if schema_name:
                if schema_name not in \
                        cmdb_utils.get_current_schema(session, self.current_user, cmdb_id):
                    self.resp_bad_req(msg=f"无法在编号为{cmdb_id}的数据库中"
                                          f"操作名为{schema_name}的schema。")
                    return

            risk_rule_q = session.query(RiskSQLRule)
            result_q = Results.objects(cmdb_id=cmdb_id, schema_name=schema_name)
            if rule_type not in (None, "ALL"):
                risk_rule_q = risk_rule_q.filter(RiskSQLRule.risk_sql_dimension == rule_type)
                result_q = result_q.filter(rule_type=rule_type)
            if risk_sql_rule_id_list:
                risk_rule_q = risk_rule_q.filter(RiskSQLRule.risk_sql_rule_id.
                                                 in_(risk_sql_rule_id_list))
            if date_start:
                result_q = result_q.filter(create_date__gte=date_start)
            if date_end:
                result_q = result_q.filter(create_date__lt=date_end)
            risky_rules = Rule.objects(
                rule_name__in=[i[0] for i in risk_rule_q.with_entities(RiskSQLRule.rule_name)],
                db_model=cmdb.db_model  # Notice: must filter db_model in rules!
            )
            risk_rule_rule_name_optimization_advice_dict = dict(risk_rule_q.with_entities(
                RiskSQLRule.rule_name,
                RiskSQLRule.optimized_advice
            ))
            risky_rule_name_object_dict = {risky_rule.rule_name:
                                               risky_rule for risky_rule in risky_rules.all()}
            if not risky_rule_name_object_dict:
                self.resp(msg="无任何风险规则。")
                return

            # 过滤出包含问题的结果
            Qs = None
            for risky_rule_name in risky_rule_name_object_dict.keys():
                if not Qs:
                    Qs = Q(**{f"{risky_rule_name}__records__nin": [None, []]})
                else:
                    Qs = Qs | Q(**{f"{risky_rule_name}__records__nin": [None, []]})
            if Qs:
                result_q = result_q.filter(Qs)

            # results, p = self.paginate(result_q, **p)
            results = result_q.all()
            risky_rule_apperance = defaultdict(dict)
            rst = []
            for result in results:
                for risky_rule_name, risky_rule_object in risky_rule_name_object_dict.items():
                    if not getattr(result, risky_rule_name, None):
                        continue  # 规则key不存在，或者值直接是个空dict
                    if not getattr(result, risky_rule_name).get("records", None):
                        continue  # 规则key存在，值非空，但是其下records的值为空
                    if not risky_rule_apperance.get(risky_rule_name):
                        # 没统计过当前rule的最早出现和最后出现时间
                        to_aggregate = [
                            {
                                "$match": {
                                    "$and": [
                                        {risky_rule_name + ".records": {"$exists": True}},
                                        {risky_rule_name + ".records": {"$not": {"$size": 0}}}
                                    ]
                                }
                            },
                            {
                                "$group": {
                                    '_id': risky_rule_name,
                                    "first_appearance": {"$min": "$create_date"},
                                    "last_appearance": {"$max": "$create_date"}
                                }
                            },
                            {
                                "$project": {
                                    '_id': 0,
                                    'first_appearance': 1,
                                    'last_appearance': 1
                                }
                            }
                        ]
                        agg_rest = list(Results.objects.aggregate(*to_aggregate))
                        risky_rule_apperance[risky_rule_name] = {
                            "first_appearance": agg_rest[0]['first_appearance']
                            if agg_rest else "",
                            "last_appearance": agg_rest[0]['last_appearance']
                            if agg_rest else ""
                        }
                    for record in getattr(result, risky_rule_name)["records"]:
                        rst.append({
                            "object_name": record[0],
                            "rule_desc": risky_rule_object.rule_desc,
                            "risk_detail": rule_utils.format_rule_result_detail(
                                risky_rule_object, record),
                            "optimized_advice": risk_rule_rule_name_optimization_advice_dict[
                                risky_rule_name],
                            **risky_rule_apperance[risky_rule_name]
                        })
            rst_this_page, p = self.paginate(rst, **p)
        self.resp(rst_this_page, **p)


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

