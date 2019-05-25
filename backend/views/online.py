# Author: kk.Fang(fkfkbill@gmail.com)

from os import path
from collections import defaultdict
from types import FunctionType

import arrow
import xlsxwriter
from schema import Schema, Optional
from mongoengine import Q

import settings
from .base import AuthReq
from backend.utils.schema_utils import *
from backend.utils import rule_utils, cmdb_utils, sql_utils
from backend.models.mongo import *
from backend.models.oracle import *


class ObjectRiskListHandler(AuthReq):

    def get_list(self, session, query_parser: FunctionType):
        # Notice：如果下面的参数修改了，必须要能同时兼容query_args和json，不然需要修改schema parse方式。
        params = query_parser(Schema({
            "cmdb_id": scm_int,
            Optional("schema_name", default=None): scm_str,
            Optional("risk_sql_rule_id", default=None): scm_dot_split_int,
            Optional("date_start", default=None): scm_date,
            Optional("date_end", default=None): scm_date,

            Optional(object): object
        }))
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

        risk_rule_q = session.query(RiskSQLRule).\
            filter(RiskSQLRule.rule_type == rule_utils.RULE_TYPE_OBJ)
        result_q = Results.objects(
            cmdb_id=cmdb_id, schema_name=schema_name, rule_type=rule_utils.RULE_TYPE_OBJ)
        if risk_sql_rule_id_list:
            risk_rule_q = risk_rule_q.filter(RiskSQLRule.risk_sql_rule_id.
                                             in_(risk_sql_rule_id_list))
        if date_start:
            result_q = result_q.filter(create_date__gte=date_start)
        if date_end:
            date_end_arrow = arrow.get(date_end)
            date_end_arrow.shift(days=+1)
            date_end = date_end_arrow.datetime
            result_q = result_q.filter(create_date__lt=date_end)
        risky_rules = Rule.objects(
            rule_name__in=[i[0] for i in risk_rule_q.with_entities(RiskSQLRule.rule_name)],
            db_model=cmdb.db_model,
            db_type=cmdb_utils.DB_ORACLE
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
                risk_rule_object = risk_rules_dict[risky_rule_object.get_3_key()]

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
                        "risk_sql_rule_id": risk_rule_object.risk_sql_rule_id,
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
            rst = self.get_list(session, self.get_query_args)
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
                object_list = self.get_list(session, self.get_json_args)
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
            filename = f"export_obj_rick_{arrow.now().format('YYYY-MM-DD-HH-mm-ss')}.xlsx"
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

    def get_list(self, session, query_parser: FunctionType):
        # Notice：如果下面的参数修改了，必须要能同时兼容query_args和json，不然需要修改schema parse方式。
        ALL_RULE_TYPES_FOR_SQL_RULE = [
            rule_utils.RULE_TYPE_TEXT,
            rule_utils.RULE_TYPE_SQLPLAN,
            rule_utils.RULE_TYPE_SQLSTAT,
        ]
        params = query_parser(Schema({
            "cmdb_id": scm_int,
            Optional("schema_name", default=None): scm_str,
            Optional("risk_sql_rule_id", default=None): scm_dot_split_int,
            Optional("date_start", default=None): scm_date,
            Optional("date_end", default=None): scm_date,

            Optional("rule_type", default="ALL"): scm_one_of_choices(["ALL"] + ALL_RULE_TYPES_FOR_SQL_RULE),
            Optional("enable_white_list", default=True): scm_bool,  # 需要注意这个字段的实际值，query_args时是0或1的字符，json时是bool
            Optional("sort_by", default="last"): scm_one_of_choices(["sum", "average"]),

            Optional(object): object
        }))
        cmdb_id = params.pop("cmdb_id")
        schema_name = params.pop("schema_name")
        risk_sql_rule_id_list = params.pop("risk_sql_rule_id")
        date_start, date_end = params.pop("date_start"), params.pop("date_end")
        rule_type = params.pop("rule_type")
        enable_white_list = params.pop("enable_white_list")  # TODO 增加过滤白名单的功能
        sort_by = params.pop("sort_by")
        del params  # shouldn't use params anymore

        cmdb = session.query(CMDB).filter_by(cmdb_id=cmdb_id).first()
        if not cmdb:
            self.resp_bad_req(msg=f"不存在编号为{cmdb_id}的cmdb。")
            return

        risk_rule_q = session.query(RiskSQLRule)
        result_q = Results.objects(cmdb_id=cmdb_id)
        if schema_name:
            if schema_name not in \
                    cmdb_utils.get_current_schema(session, self.current_user, cmdb_id):
                self.resp_bad_req(msg=f"无法在编号为{cmdb_id}的数据库中"
                f"操作名为{schema_name}的schema。")
                return
            result_q = result_q.filter(schema_name=schema_name)
        if rule_type == "ALL":
            rule_type: list = ALL_RULE_TYPES_FOR_SQL_RULE
        else:
            rule_type: list = [rule_type]
        risk_rule_q = risk_rule_q.filter(RiskSQLRule.rule_type.in_(rule_type))
        result_q = result_q.filter(rule_type__in=rule_type)

        if risk_sql_rule_id_list:
            risk_rule_q = risk_rule_q.filter(RiskSQLRule.risk_sql_rule_id.
                                             in_(risk_sql_rule_id_list))
        if date_start:
            result_q = result_q.filter(create_date__gte=date_start)
        if date_end:
            date_end_arrow = arrow.get(date_end)
            date_end_arrow.shift(days=+1)
            date_end = date_end_arrow.datetime
            result_q = result_q.filter(create_date__lt=date_end)
        risky_rules = Rule.objects(
            rule_name__in=[i[0] for i in risk_rule_q.with_entities(RiskSQLRule.rule_name)],
            db_model=cmdb.db_model,
            db_type=cmdb_utils.DB_ORACLE
        )
        risk_rules_dict = rule_utils.get_risk_rules_dict(session)
        risky_rule_name_object_dict = {risky_rule.rule_name:
                                           risky_rule for risky_rule in risky_rules.all()}
        if not risky_rule_name_object_dict:
            self.resp(msg="无任何风险规则。")
            return

        # 过滤出包含风险SQL规则结果的result
        Qs = None
        for risky_rule_name in risky_rule_name_object_dict.keys():
            if not Qs:
                Qs = Q(**{f"{risky_rule_name}__sqls__nin": [None, []]})
            else:
                Qs = Qs | Q(**{f"{risky_rule_name}__sqls__nin": [None, []]})
        if Qs:
            result_q = result_q.filter(Qs)
        results = result_q.all()
        sql_text_stats = sql_utils.get_sql_id_stats(cmdb_id)
        rst = []
        rst_sql_id_set = set()
        for result in results:

            # result具有可变字段，具体结构请参阅models.mongo.results

            for risky_rule_name, risky_rule_object in risky_rule_name_object_dict.items():
                risk_rule_object = risk_rules_dict[risky_rule_object.get_3_key()]

                # risky_rule_object is a record of Rule from mongodb

                # risk_rule_object is a record of RiskSQLRule from oracle

                if not getattr(result, risky_rule_name, None):
                    continue  # 规则key不存在，或者值直接是个空dict，则跳过
                if not getattr(result, risky_rule_name).get("sqls", None):
                    # 规则key下的sqls不存在，或者值直接是个空list，则跳过
                    # e.g. {"XXX_RULE_NAME": {"scores": 0.0}}  # 无sqls
                    # e.g. {"XXX_RULE_NAME": {"sqls": [], "scores": 0.0}}
                    continue

                sqls = getattr(result, risky_rule_name)["sqls"]

                for sql_text_dict in sqls:
                    sql_text_dict_stat = sql_text_dict.get("stat", {})
                    sql_id = sql_text_dict["sql_id"]
                    if sql_id in rst_sql_id_set:
                        continue
                    execution_time_cost_sum = round(sql_text_dict_stat["ELAPSED_TIME_DELTA"], 2)
                    execution_times = sql_text_dict_stat.get('EXECUTIONS_DELTA', 0)
                    execution_time_cost_on_average = round(execution_time_cost_sum / execution_times, 2)
                    rst.append({
                        "sql_id": sql_id,
                        "schema": sql_text_dict["schema"],
                        "sql_text": sql_text_dict["sql_text"],
                        "rule_desc": risky_rule_object.rule_desc,
                        "severity": risk_rule_object.severity,
                        "first_appearance": sql_text_stats[sql_id]['first_appearance'],
                        "last_appearance": sql_text_stats[sql_id]['last_appearance'],
                        "similar_sql_num": 1,  # sql_text_stats[sql_id]["count"],  # TODO 这是啥？
                        "execution_time_cost_sum": execution_time_cost_sum,
                        "execution_times": execution_times,
                        "execution_time_cost_on_average": execution_time_cost_on_average,
                        "risk_sql_rule_id": risk_rule_object.risk_sql_rule_id
                    })
                    rst_sql_id_set.add(sql_id)
        if sort_by == "sum":
            rst = sorted(rst, key=lambda x: x["execution_time_cost_sum"], reverse=True)
        if sort_by == "average":
            rst = sorted(rst, key=lambda x: x["execution_time_cost_on_average"], reverse=True)
        return rst

    def get(self):
        """风险SQL列表"""
        params = self.get_query_args(Schema({
            Optional("page", default=1): scm_int,
            Optional("per_page", default=10): scm_int,

            Optional(object): object
        }))
        p = self.pop_p(params)
        del params  # shouldn't use params anymore

        with make_session() as session:
            rst = self.get_list(session, self.get_query_args)
            if rst is None:
                return
            rst_this_page, p = self.paginate(rst, **p)
        self.resp(rst_this_page, **p)


class SQLRiskReportExportHandler(SQLRiskListHandler):

    def post(self):
        """导出风险SQL的报告"""
        self.resp_created()


class SQLRiskDetailHandler(AuthReq):

    def get(self):
        """风险详情（include sql text, sql plan and statistics）"""
        params = self.get_query_args(Schema({
            "cmdb_id": scm_int,
            "sql_id": scm_str,
            "risk_sql_rule_id": scm_dot_split_int,
            Optional("date_start", default=None): scm_date,
            Optional("date_end", default=None): scm_date,
        }))
        cmdb_id = params.pop("cmdb_id")
        sql_id = params.pop("sql_id")
        risk_rule_id_list: list = params.pop("risk_id")
        date_start = params.pop("date_start")
        date_end = params.pop("date_end")
        del params  # shouldn't use params anymore
        with make_session() as session:
            risk_rules = session.query(RiskSQLRule).filter(RiskSQLRule.risk_sql_rule_id.
                                                           in_(risk_rule_id_list))
            sql_text_stats = sql_utils.get_sql_id_stats(cmdb_id)
            latest_sql_text_object = SQLText.objects(sql_id=sql_id).order_by("-etl_date").first()

            # query graph
            graphs = {
                'cpu_time_delta': defaultdict(list),
                'disk_reads_delta': defaultdict(list),
                'elapsed_time_delta': defaultdict(list),
                'etl_date': [],
            }
            sql_stat_objects = SQLStat.objects(sql_id=sql_id)
            if date_start:
                sql_stat_objects = sql_stat_objects.filter(etl_date__gte=date_start)
            if date_end:
                date_end_arrow = arrow.get(date_end)
                date_end_arrow.shift(days=+1)
                date_end = date_end_arrow.datetime
                sql_stat_objects = sql_stat_objects.filter(etl_date__lte=date_end)
            for sql_stat_obj in sql_stat_objects:
                graphs['cpu_time_delta'][str(sql_stat_obj.plan_hash_value)].\
                    append(round(sql_stat_obj.cpu_time_dalta, 2))
                graphs['elapsed_time_delta'][str(sql_stat_obj['PLAN_HASH_VALUE'])].\
                    append(round(sql_stat_obj.elapsed_time_delta, 2))
                graphs['disk_reads_delta'][str(sql_stat_obj['PLAN_HASH_VALUE'])].\
                    append(round(sql_stat_obj.disk_reads_delta, 2))
                graphs['etl_date'].append(sql_stat_obj.etl_date)

            # query sql plan
            hash_values = MSQLPlan.objects(sql_id=params['sql_id'], cmdb_id=params['cmdb_id']).distinct(
            MSQLPlan.plan_hash_value)
            plans=get_plans({"SQL_ID": params['sql_id'], 'cmdb_id': params['cmdb_id']})
            plans = [[
                hash_value,
                plans['cost'],
                plans['first_time'],
                plans['last_time'],
            ] for hash_value in hash_values]

            self.resp({
                'sql_id': sql_id,
                'schema': sql_stat_obj.schema,
                'first_appearance': sql_text_stats[sql_id]["first_appearance"],
                'last_appearance': sql_text_stats[sql_id]["last_appearance"],
                'sql_text': latest_sql_text_object.sql_text,
                'risk_rules': [rr.to_dict() for rr in risk_rules],
                'graph': graphs,
                'plans': plans,
            })


class SQLPlanHandler(AuthReq):

    def get(self):
        """风险详情的sql plan详情"""
        self.resp()
