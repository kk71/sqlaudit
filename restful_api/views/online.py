# Author: kk.Fang(fkfkbill@gmail.com)

from os import path
from collections import defaultdict
from types import FunctionType

import sqlparse
import xlsxwriter
from schema import Schema, Optional, And
from mongoengine import Q

from utils.const import *
from utils.perf_utils import *
from .base import AuthReq
from utils.schema_utils import *
from utils.datetime_utils import *
from utils import rule_utils, cmdb_utils, sql_utils, object_utils, score_utils
from models.oracle import *
from models.mongo import *


class ObjectRiskListHandler(AuthReq):

    @classmethod
    def parsing_schema_dict(cls):
        """给接口用的schema，防止反复写"""
        return {
            "cmdb_id": scm_int,
            Optional("schema_name", default=None): scm_str,
            Optional("risk_sql_rule_id", default=None): scm_dot_split_int,
            Optional("date_start", default=None): scm_date,
            Optional("date_end", default=None): scm_date,

            Optional(object): object
        }

    def get(self):
        """风险列表"""
        params = self.get_query_args(Schema({
            **self.parsing_schema_dict(),

            Optional("page", default=1): scm_int,
            Optional("per_page", default=10): scm_int,
        }))
        p = self.pop_p(params)

        with make_session() as session:
            rst = object_utils.get_risk_object_list(session, **params)
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
                params = self.get_json_args(Schema(**self.parsing_schema_dict()))
                object_list = object_utils.get_risk_object_list(session, **params)

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
                            Optional(object): object
                        }
                    ],

                    Optional(object): object
                }))
                object_list = params.pop("objects")
                del params  # shouldn't use params anymore

            else:
                assert 0

            heads = ['对象名称', "风险等级", '风险点', '风险详情', '最早出现时间', '最后出现时间', '优化建议']
            filename = f"export_obj_risk_{arrow.now().format(COMMON_DATETIME_FORMAT)}.xlsx"
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

    @classmethod
    def parsing_schema_dict(cls):
        """给接口用的schema，防止反复写"""
        return {
            "cmdb_id": scm_int,
            Optional("schema_name", default=None): scm_str,
            Optional("risk_sql_rule_id", default=None): scm_dot_split_int,
            Optional("date_start", default=None): scm_date,
            Optional("date_end", default=None): scm_date,
            Optional("rule_type", default="ALL"): scm_one_of_choices(
                ["ALL"] + ALL_RULE_TYPES_FOR_SQL_RULE),
            Optional("enable_white_list", default=True):
                scm_bool,  # 需要注意这个字段的实际值，query_args时是0或1的字符，json时是bool
            Optional("sort_by", default="last"): scm_one_of_choices(["sum", "average"]),
        }

    def get(self):
        """风险SQL列表"""
        params = self.get_query_args(Schema({
            **self.parsing_schema_dict(),

            Optional("page", default=1): scm_int,
            Optional("per_page", default=10): scm_int,
        }))
        p = self.pop_p(params)
        date_range = params.pop("date_start"), params.pop("date_end")

        with make_session() as session:
            try:
                rst = sql_utils.get_risk_sql_list(session, **params, date_range=date_range)
            except NoRiskRuleSetException:
                self.resp(msg="未设置风险规则。")
                return
            rst_this_page, p = self.paginate(rst, **p)
        self.resp(rst_this_page, **p)


class SQLRiskReportExportHandler(SQLRiskListHandler):

    def post(self):
        """导出风险SQL的报告"""
        params = self.get_json_args(Schema({
            "export_type": scm_one_of_choices(["all_filtered", "selected"]),

            Optional(object): object
        }))
        export_type = params.pop("export_type")

        with make_session() as session:
            if export_type == "all_filtered":
                params = self.get_json_args(Schema({
                    **self.parsing_schema_dict(),

                    Optional(object): object
                }))
                date_range = params.pop("date_start"), params.pop("date_end")
                sql_list = sql_utils.get_risk_sql_list(session, **params, date_range=date_range)

            elif export_type == "selected":
                params = self.get_json_args(Schema({
                    "objects": [
                        {
                            "schema": scm_unempty_str,
                            "sql_id": scm_unempty_str,
                            "rule_desc": scm_unempty_str,
                            "first_appearance": scm_str,
                            "last_appearance": scm_str,
                            "similar_sql_num": scm_int,
                            "execution_time_cost_sum": object,
                            "execution_times": object,
                            "execution_time_cost_on_average": object,
                            "sql_text": scm_str,
                            Optional(object): object
                        }
                    ],

                    Optional(object): object
                }))
                sql_list = params.pop("objects")
                del params  # shouldn't use params anymore

            else:
                assert 0

            heads = ['执行用户', 'SQL_ID', '风险点', '最早出现时间', '最后出现时间', '相似SQL',
                     '上次执行总时间', '上次执行次数', '上次平均时间', 'SQL文本']
            filename = f"export_sql_risk_{arrow.now().format(COMMON_DATETIME_FORMAT)}.xlsx"
            full_filename = path.join(settings.EXPORT_DIR, filename)
            wb = xlsxwriter.Workbook(full_filename)
            ws = wb.add_worksheet('风险SQL报告')
            format_title = wb.add_format({
                'bold': 1,
                'size': 14,
                'align': 'center',
                'valign': 'vcenter',

            })
            format_text = wb.add_format({
                'align': 'left',
                'valign': 'vcenter',
                'text_wrap': True,
            })
            ws.set_column(0, 2, 20)
            ws.set_column(3, 4, 18)
            ws.set_column(5, 5, 10)
            ws.set_column(6, 8, 18)
            ws.set_column(9, 9, 50)
            ws.set_row(0, 30)
            [ws.write(0, x, field.upper(), format_title) for x, field in enumerate(heads)]
            for row_num, row in enumerate(sql_list):
                sql_text = sqlparse.format(row["sql_text"], reindent=True, keyword_case='upper')
                row_num += 1
                ws.write(row_num, 0, row["schema"], format_text)
                ws.write(row_num, 1, row["sql_id"], format_text)
                ws.write(row_num, 2, row["rule_desc"], format_text)
                ws.write(row_num, 3, row["first_appearance"], format_text)
                ws.write(row_num, 4, row["last_appearance"], format_text)
                ws.write(row_num, 5, row["similar_sql_num"], format_text)
                ws.write(row_num, 6, row["execution_time_cost_sum"], format_text)
                ws.write(row_num, 7, row["execution_times"], format_text)
                ws.write(row_num, 8, row["execution_time_cost_on_average"], format_text)
                ws.write(row_num, 9, sql_text, format_text)
            wb.close()
            self.resp({"url": path.join(settings.EXPORT_PREFIX, filename)})


class SQLRiskDetailHandler(AuthReq):

    @timing()
    def get(self):
        """风险详情（include sql text, sql plan and statistics）"""
        params = self.get_query_args(Schema({
            "cmdb_id": scm_int,
            "sql_id": scm_str,
            Optional("risk_sql_rule_id", default=None): scm_dot_split_int,
            Optional("date_start", default=None): scm_date,
            Optional("date_end", default=None): scm_date,
        }))
        cmdb_id = params.pop("cmdb_id")
        sql_id = params.pop("sql_id")
        risk_rule_id_list: list = params.pop("risk_sql_rule_id")
        date_start = params.pop("date_start")
        date_end = params.pop("date_end")
        del params  # shouldn't use params anymore

        with make_session() as session:
            risk_rules = session.query(RiskSQLRule)
            if risk_rule_id_list:
                # 给出了risk sql id就好办
                risk_rules = risk_rules.filter(
                    RiskSQLRule.risk_sql_rule_id.in_(risk_rule_id_list))
            else:
                rule_names = rule_utils.get_all_risk_towards_a_sql(
                    session,
                    sql_id,
                    date_range=(date_start, date_end)
                )
                risk_rules = risk_rules.filter(RiskSQLRule.rule_name.in_(rule_names))

            sql_text_stats = sql_utils.get_sql_id_stats(cmdb_id)
            latest_sql_text_object = SQLText.objects(sql_id=sql_id).\
                order_by("-etl_date").\
                first()
            if not latest_sql_text_object:
                self.resp_not_found(msg="不存在该SQL")
                return
            schemas = list(set(MSQLPlan.objects(sql_id=sql_id, cmdb_id=cmdb_id).
                               distinct("schema")))

            hash_values = set(MSQLPlan.objects(sql_id=sql_id, cmdb_id=cmdb_id).
                              distinct("plan_hash_value"))
            sql_plan_stats = sql_utils.get_sql_plan_stats(cmdb_id)
            plans = [
                # {check codes blow for structure details}
            ]
            graphs = {
                plan_hash_value: {
                    'cpu_time_delta': defaultdict(list),
                    'disk_reads_delta': defaultdict(list),
                    'elapsed_time_delta': defaultdict(list),
                } for plan_hash_value in hash_values
            }
            for plan_hash_value in hash_values:
                # plans
                sql_plan_object = MSQLPlan.objects(cmdb_id=cmdb_id, sql_id=sql_id,
                                                   plan_hash_value=plan_hash_value).first()
                plans.append({
                    "plan_hash_value": plan_hash_value,
                    "cost": sql_plan_object.cost,
                    "first_appearance": dt_to_str(sql_plan_stats[plan_hash_value]["first_appearance"]),
                    "last_appearance": dt_to_str(sql_plan_stats[plan_hash_value]["last_appearance"]),
                })
                # stats
                sql_stat_objects = SQLStat.objects(cmdb_id=cmdb_id, sql_id=sql_id,
                                                   plan_hash_value=plan_hash_value)
                if date_start:
                    sql_stat_objects = sql_stat_objects.filter(etl_date__gte=date_start)
                if date_end:
                    sql_stat_objects = sql_stat_objects.filter(etl_date__lte=date_end)
                for sql_stat_obj in sql_stat_objects:
                    gp = graphs[plan_hash_value]
                    etl_date_str = dt_to_str(sql_stat_obj.etl_date)
                    gp['cpu_time_delta'][str(sql_stat_obj.plan_hash_value)].append({
                        "date": etl_date_str,
                        "value": round(sql_stat_obj.cpu_time_delta, 2)
                    })
                    gp['elapsed_time_delta'][str(sql_stat_obj.plan_hash_value)].append({
                        "date": etl_date_str,
                        "value": round(sql_stat_obj.elapsed_time_delta, 2)
                    })
                    gp['disk_reads_delta'][str(sql_stat_obj.plan_hash_value)].append({
                        "date": etl_date_str,
                        "value": round(sql_stat_obj.disk_reads_delta, 2)
                    })

            self.resp({
                'sql_id': sql_id,
                'schema': schemas[0] if schemas else None,
                'first_appearance': dt_to_str(sql_text_stats[sql_id]["first_appearance"]),
                'last_appearance': dt_to_str(sql_text_stats[sql_id]["last_appearance"]),
                'sql_text': latest_sql_text_object.sql_text,
                'risk_rules': [rr.to_dict() for rr in risk_rules],
                'graph': graphs,
                'plans': plans,
            })


class SQLPlanHandler(AuthReq):

    def get(self):
        """风险详情的sql plan详情"""
        params = self.get_query_args(Schema({
            "sql_id": scm_unempty_str,
            "plan_hash_value": scm_int,
            "cmdb_id": scm_int,
        }))
        plans = MSQLPlan.objects(**params).order_by("-etl_date")
        filtered_plans = []
        indices_set = set()
        for p in plans:
            if p.index not in indices_set:
                filtered_plans.append(p.to_dict(iter_if=lambda k, v: k in (
                    "depth",
                    "operation",
                    "object_owner",
                    "object_name",
                    "position",
                    "cost",
                    "time",
                    "access_predicates",
                    "filter_predicates"
                )))
                indices_set.add(p.index)
        filtered_plans = sorted(filtered_plans, key=lambda x: x["depth"])
        self.resp(filtered_plans)


class TableInfoHandler(AuthReq):

    def get(self):
        params = self.get_query_args(Schema({
            "cmdb_id": scm_int,
            "schema_name": scm_unempty_str,
            "table_name": scm_unempty_str
        }))
        latest_tab_info = ObjTabInfo.objects(table_name=params["table_name"]).\
            order_by("-etl_date").first()
        if not latest_tab_info:
            self.resp({}, msg="无数据。")
            return
        params["record_id"] = latest_tab_info.record_id
        self.resp({
            'basic': [i.to_dict(iter_if=lambda k, v: k in (
                "schema_name", "table_name", "table_type", "iot_name", "num_rows",
                "blocks", "avg_row_len", "last_analyzed", "last_ddl_date",
                "chain_cnt", "hwm_stat", "compression", "phy_size_mb"
            )) for i in ObjTabInfo.objects(**params)],

            'detail': [i.to_dict(iter_if=lambda k, v: k in (
                "schema_name", "table_name", "column_name", "data_type", "nullable",
                "num_nulls", "num_distinct", "data_default", "avg_col_len"
            )) for i in ObjTabCol.objects(**params)],

            'partition': [i.to_dict(iter_if=lambda k, v: k in (
                "schema_name", "table_name", "partitioning_type", "column_name",
                "partitioning_key_count", "partition_count",
                "sub_partitioning_key_count", "sub_partitioning_type",
                "last_ddl_date", "phy_size_mb", "num_rows"
            )) for i in ObjPartTabParent.objects(**params)],

            'index': [i.to_dict(iter_if=lambda k, v: k in (
                "schema_name", "index_name", "table_owner", "table_name", "column_name",
                "column_position", "descend"
            )) for i in ObjIndColInfo.objects(**params)],
            'view': [i.to_dict(iter_if=lambda k, v: k in (
                "obj_pk", "schema_name", "view_name", "referenced_owner", "referenced_name",
                "referenced_type"
            )) for i in ObjViewInfo.objects(**params)]
        })


class OverviewHandler(SQLRiskListHandler):

    @timing()
    def get(self):
        """风险详情的sql plan详情"""
        """数据库健康度概览"""
        params = self.get_query_args(Schema({
            "cmdb_id": scm_int,
            Optional("schema_name", default=None): scm_str,
            "date_start": scm_date,
            "date_end": scm_date,
        }))
        cmdb_id = params.pop("cmdb_id")
        schema_name = params.pop("schema_name")
        date_start = params.pop("date_start")
        date_end = params.pop("date_end")
        del params  # shouldn't use params anymore

        with make_session() as session:
            dt_now = arrow.get(date_start)
            dt_end = arrow.get(date_end)
            sql_num_active = []
            sql_num_at_risk = []

            # SQL count
            while dt_now <= dt_end:
                sql_text_q = SQLText.objects(
                    cmdb_id=cmdb_id,
                    etl_date__gte=dt_now.datetime,
                    etl_date__lt=dt_now.shift(days=+1).datetime,
                )
                if schema_name:
                    sql_text_q = sql_text_q.filter(schema=schema_name)
                active_sql_num = len(sql_text_q.distinct("sql_id"))
                at_risk_sql_num = len(sql_utils.get_risk_sql_list(
                    session,
                    cmdb_id=cmdb_id,
                    schema_name=schema_name,
                    sql_id_only=True,
                    date_range=(dt_now.datetime, dt_now.shift(days=+1).datetime)
                ))
                sql_num_active.append({
                    "date": dt_to_str(dt_now),
                    "value": active_sql_num
                })
                sql_num_at_risk.append({
                    "date": dt_to_str(dt_now),
                    "value": at_risk_sql_num
                })
                dt_now = dt_now.shift(days=+1)

            # risk_rule_rank

            # 只需要拿到rule_name即可，不需要知道其他两个key,
            # 因为当前仅对某一个库做分析，数据库类型和db_model都是确定的
            risk_rule_name_sql_num_dict = {
                # rule_name: {...}
                r3key[2]: {
                    "violation_num": 0,
                    "schema_set": set(),
                    **robj.to_dict(iter_if=lambda k, v: k in ("risk_name", "severity"))
                }
                for r3key, robj in rule_utils.get_risk_rules_dict(session).items()}
            results_q = Results.objects(
                cmdb_id=cmdb_id, create_date__gte=date_start, create_date__lte=date_end)
            if schema_name:
                results_q = results_q.filter(schema_name=schema_name)
            for result in results_q:
                for rule_name in risk_rule_name_sql_num_dict.keys():
                    result_rule_dict = getattr(result, rule_name, None)
                    if not result_rule_dict:
                        continue
                    if result_rule_dict.get("records", []) or result_rule_dict.get("sqls", []):
                        risk_rule_name_sql_num_dict[rule_name]["violation_num"] += 1
                        risk_rule_name_sql_num_dict[rule_name]["schema_set"].\
                            add(result.schema_name)
            risk_rule_rank = [
                {
                    "rule_name": rule_name,
                    "num": k["violation_num"],
                    "risk_name": k["risk_name"],
                    "severity": k["severity"],
                } for rule_name, k in risk_rule_name_sql_num_dict.items()
            ]

            risk_rule_rank = sorted(risk_rule_rank, key=lambda x: x["num"], reverse=True)

            # top 10 execution cost by sum and by average
            sqls = sql_utils.get_risk_sql_list(
                session,
                cmdb_id=cmdb_id,
                schema_name=schema_name,
                date_range=(date_start, date_end),
                sqltext_stats=False
            )
            sql_by_sum = [
                {"sql_id": sql["sql_id"], "time": sql["execution_time_cost_sum"]}
                for sql in sqls
            ]
            top_10_sql_by_sum = sorted(
                sql_by_sum,
                key=lambda x: x["time"],
                reverse=True
            )[:10]
            top_10_sql_by_sum.reverse()
            sql_by_average = [
                {"sql_id": sql["sql_id"], "time": sql["execution_time_cost_on_average"]}
                for sql in sqls
            ]
            top_10_sql_by_average = sorted(
                sql_by_average,
                key=lambda x: x["time"],
                reverse=True
            )[:10]
            top_10_sql_by_average.reverse()

            # physical size of current CMDB
            cmdb = session.query(CMDB).filter_by(cmdb_id=cmdb_id).first()
            phy_size = object_utils.get_cmdb_phy_size(session, cmdb)

            self.resp({
                # 以下是按照给定的时间区间搜索的结果
                "sql_num": {"active": sql_num_active, "at_risk": sql_num_at_risk},
                "risk_rule_rank": risk_rule_rank,
                "sql_execution_cost_rank": {
                    "by_sum": top_10_sql_by_sum,
                    "by_average": top_10_sql_by_average
                },
                "risk_rates": rule_utils.get_risk_rate(cmdb_id, (date_start, date_end)),
                # 以下是取最近一次扫描的结果
                "phy_size_mb": phy_size,
            })


class OverviewScoreByHandler(AuthReq):

    def get(self):
        """显示整个库评分(按照schema或者rule_type)的(柱状图或者雷达图)"""
        params = self.get_query_args(Schema({
            "cmdb_id": scm_int,
            "perspective": scm_one_of_choices(ALL_OVERVIEW_ITEM),
            Optional("score_type", default=None): And(
                scm_int,
                scm_one_of_choices(ALL_SCORE_BY)
            )
        }))
        score_type = params.pop("score_type")
        perspective = params.pop("perspective")
        with make_session() as session:
            cmdb = session.query(CMDB).filter_by(**params).first()
            overview_rate = session.query(OverviewRate).filter_by(
                login_user=self.current_user,
                cmdb_id=cmdb.cmdb_id,
                item=perspective
            ).order_by(OverviewRate.id.desc()).first()
            if score_type is None:
                if overview_rate:
                    score_type = overview_rate.type
                else:
                    score_type = SCORE_BY_LOWEST
            elif score_type and not overview_rate:
                overview_rate = OverviewRate(
                    login_user=self.current_user,
                    cmdb_id=cmdb.cmdb_id,
                    item=perspective,
                    type=score_type)
                session.add(overview_rate)
            elif score_type and overview_rate:
                overview_rate.type = score_type
                session.add(overview_rate)
            ret = score_utils.calc_score_by(session, cmdb, perspective, score_type)
            if perspective == OVERVIEW_ITEM_SCHEMA:
                d = sorted(
                    self.dict_to_verbose_dict_in_list(ret, "schema", "num"),
                    key=lambda k: k["num"]
                )
            elif perspective == OVERVIEW_ITEM_RADAR:
                d = ret
            else:
                assert 0
            self.resp({
                "data": d,
                "score_type": score_type
            })
