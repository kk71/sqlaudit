# Author: kk.Fang(fkfkbill@gmail.com)

from os import path
from collections import defaultdict

import arrow
import xlsxwriter
from schema import Schema, Optional
from mongoengine import Q

import settings
from .base import AuthReq
from backend.utils.schema_utils import *
from backend.utils import rule_utils, cmdb_utils
from backend.models.mongo import Rule, Results
from backend.models.oracle import *


class ObjectRiskListHandler(AuthReq):

    def get_list(self, session):
        params = self.get_query_args(Schema({
            Optional("type", default="ALL"): scm_one_of_choices(["ALL"] +
                                                                list(rule_utils.ALL_RULE_TYPE)),
            "cmdb_id": scm_int,
            Optional("schema_name", default=None): scm_str,
            Optional("risk_sql_rule_id", default=None): scm_dot_split_int,
            Optional("date_start", default=None): scm_date,
            Optional("date_end", default=None): scm_date,

            Optional(object): object
        }))
        rule_type = params.pop("type")
        cmdb_id = params.pop("cmdb_id")
        schema_name = params.pop("schema_name")
        risk_sql_rule_id_list = params.pop("risk_sql_rule_id")
        date_start, date_end = params.pop("date_start"), params.pop("date_end")
        del params  # shouldn't use params anymore

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
            risk_rule_q = risk_rule_q.filter(RiskSQLRule.rule_type == rule_type)
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
        risk_rules_dict = rule_utils.get_risk_rules_dict(session)
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
        risky_rule_appearance = defaultdict(dict)
        rst = []
        for result in results:
            for risky_rule_name, risky_rule_object in risky_rule_name_object_dict.items():
                risk_rule_object = risk_rules_dict[risky_rule_object.get_four_key()]

                # risky_rule_object is a record of Rule from mongodb

                # risk_rule_object is a record of RiskSQLRule from oracle

                if not getattr(result, risky_rule_name, None):
                    continue  # 规则key不存在，或者值直接是个空dict
                if not getattr(result, risky_rule_name).get("records", None):
                    continue  # 规则key存在，值非空，但是其下records的值为空
                if not risky_rule_appearance.get(risky_rule_name):
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
                    risky_rule_appearance[risky_rule_name] = {
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
                        "optimized_advice": risk_rule_object.optimized_advice,
                        "severity": risk_rule_object.severity,
                        **risky_rule_appearance[risky_rule_name]
                    })
        return rst

    def get(self):
        """风险列表"""
        params = self.get_query_args(Schema({
            Optional("page", default=1): scm_int,
            Optional("per_page", default=10): scm_int,

            Optional(object): object
        }))
        p = self.pop_p(params)
        del params  # shouldn't use params anymore

        with make_session() as session:
            rst = self.get_list(session)
            if rst is None:
                return
            rst_this_page, p = self.paginate(rst, **p)
        self.resp(rst_this_page, **p)


class ObjectRiskReportExportHandler(ObjectRiskListHandler):

    def post(self):
        """导出风险对象报告"""
        params = self.get_json_args(Schema({
            "export_type": scm_one_of_choices(["all_filtered", "selected"]),

            Optional(object): object
        }))
        export_type = params.pop("export_type")
        del params  # shouldn't use params anymore

        with make_session() as session:
            if export_type == "all_filtered":
                object_list = self.get_list(session)
                if object_list is None:
                    return

            elif export_type == "selected":
                params = self.get_json_args(Schema({
                    "objects": [
                        {
                            "object_name": scm_unempty_str,
                            "rule_desc": scm_unempty_str,
                            "risk_detail": scm_unempty_str,
                            "optimized_advice": scm_unempty_str,
                            "first_appearance": scm_unempty_str,
                            "last_appearance": scm_unempty_str,
                            "severity": scm_unempty_str,
                        }
                    ],

                    Optional(object): object
                }))
                object_list = params.pop("objects")
                del params  # shouldn't use params anymore

            else:
                assert 0

            heads = ['对象名称', "风险等级", '风险点', '风险详情', '最早出现时间', '最后出现时间', '优化建议']
            filename = f"export_obj_rick_{arrow.now().timestamp}.xlsx"
            full_filename = path.join(settings.EXPORT_DIR, filename)
            wb = xlsxwriter.Workbook(full_filename)
            ws = wb.add_worksheet('风险对象报告')
            title_format = wb.add_format({
                'size': 14,
                'bold': 1,
                'align': 'center',
                'valign': 'vcenter',
            })
            content_format = wb.add_format({
                'align': 'left',
                'valign': 'vcenter',
                'text_wrap': True,
            })
            ws.set_row(0, 20, title_format)
            ws.set_column(0, 0, 18)
            ws.set_column(1, 1, 20)
            ws.set_column(2, 2, 40)
            ws.set_column(3, 4, 16)
            ws.set_column(5, 5, 50)
            [ws.write(0, x, field, title_format) for x, field in enumerate(heads)]
            for row_num, row in enumerate(object_list):  # [[], ...]
                row_num += 1
                ws.write(row_num, 0, row["object_name"], content_format)
                ws.write(row_num, 1, row["severity"], content_format)
                ws.write(row_num, 2, row["rule_desc"], content_format)
                ws.write(row_num, 3, row["risk_detail"], content_format)
                ws.write(row_num, 4, row["optimized_advice"], content_format)
                ws.write(row_num, 5, row["first_appearance"], content_format)
                ws.write(row_num, 6, row["last_appearance"], content_format)
            wb.close()
            self.resp({"url": path.join(settings.EXPORT_PREFIX, filename)})


class SQLRiskListHandler(AuthReq):

    def get(self):
        """风险SQL列表"""
        self.resp()


class SQLRiskReportExportHandler(SQLRiskListHandler):

    def post(self):
        """导出风险SQL的报告"""
        self.resp_created()


class SQLRiskDetailHandler(AuthReq):

    def get(self):
        """风险详情（include sql text, sql plan and statistics）"""
        self.resp()


class SQLPlanHandler(AuthReq):

    def get(self):
        """风险详情的sql plan详情"""
        self.resp()
