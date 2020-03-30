# Author: kk.Fang(fkfkbill@gmail.com)

from os import path
from collections import defaultdict
from prettytable import PrettyTable

import re
import sqlparse
import xlsxwriter
from schema import Schema, Optional, And

import settings
from utils.conc_utils import *
from utils.const import *
from utils.perf_utils import *
from .base import AuthReq, PrivilegeReq
from utils.schema_utils import *
from utils.datetime_utils import *
from utils import rule_utils, sql_utils, object_utils, score_utils
from utils.sql_utils import risk_sql_export_data
from utils.object_utils import risk_object_export_data
from models.oracle import *
from models.mongo import *
from models.mongo.statistics import StatsRiskObjectsRule, StatsRiskSqlRule


class ObjectRiskListHandler(AuthReq):

    @classmethod
    def parsing_schema_dict(cls):
        """给接口用的schema，防止反复写"""
        return {
            "cmdb_id": scm_int,
            Optional("schema_name", default=None): scm_str,
            Optional("risk_sql_rule_id", default=None): scm_dot_split_int,
            Optional("rule_name", default=None): scm_str,
            Optional("severity", default=None): scm_dot_split_str,
            scm_optional("task_record_id"): scm_int,
            scm_optional("date_start"): scm_date,
            scm_optional("date_end"): scm_date_end,

            Optional(object): object
        }

    async def get(self):
        """风险列表"""
        params = self.get_query_args(Schema({
            **self.parsing_schema_dict(),

            Optional("page", default=1): scm_int,
            Optional("per_page", default=10): scm_int,
        }))
        p = self.pop_p(params)

        with make_session() as session:
            rst = await AsyncTimeout(60).async_thr(
                object_utils.get_risk_object_list, session=session, **params)
            rst_this_page, p = self.paginate(rst, **p)
        self.resp(rst_this_page, **p)


class ObjectRiskRuleHandler(AuthReq):

    def get(self):
        """风险对象外层规则,违反规则个数等"""
        params = self.get_query_args(Schema({
            "cmdb_id": scm_int,
            Optional("schema"): scm_str,
            Optional("rule_name", default=None): scm_dot_split_str,
            Optional("severity"): scm_str,
            "date_start": scm_date,
            "date_end": scm_date_end,

            Optional("page", default=1): scm_int,
            Optional("per_page", default=10): scm_int,
        }))
        p = self.pop_p(params)
        date_start = params.pop("date_start")
        date_end = params.pop("date_end")
        rule_name: Union[list, None] = params.pop("rule_name")

        risk_obj_rule = StatsRiskObjectsRule.objects(**params).order_by("-etl_date")
        if date_start:
            risk_obj_rule = risk_obj_rule.filter(etl_date__gte=date_start)
        if date_end:
            risk_obj_rule = risk_obj_rule.filter(etl_date__lte=date_end)
        if rule_name:
            risk_obj_rule = risk_obj_rule.filter(rule__rule_name__in=rule_name)
        risk_obj_rule = [x.to_dict() for x in risk_obj_rule]
        rst_this_page, p = self.paginate(risk_obj_rule, **p)

        self.resp(rst_this_page, **p)


class ObjectRiskExportReportHandler(AuthReq):

    async def post(self):
        """风险对象列表导出v2"""
        params = self.get_json_args(Schema({
            "cmdb_id": scm_int,
            "date_start": scm_date,
            "date_end": scm_date_end,
            Optional("schema", default=None): scm_str,
            Optional("severity", default=None): list,
            Optional("rule_name", default=None): list,
            Optional("_id", default=None): list
        }))
        cmdb_id = params.pop("cmdb_id")
        schema: Union[str, None] = params.pop("schema")
        date_start, date_end = params.pop("date_start"), params.pop("date_end")
        rule_name: Union[list, None] = params.pop("rule_name")
        severity: Union[list, None] = params.pop("severity")
        ids: Union[list, None] = params.pop("_id")
        del params

        rr, rst = await async_thr(risk_object_export_data, cmdb_id=cmdb_id, schema=schema,
                                  date_start=date_start, date_end=date_end,
                                  severity=severity, rule_name=rule_name,
                                  ids=ids)

        filename = f"risk_obj_risk_{arrow.now().format(COMMON_DATETIME_FORMAT)}.xlsx"
        full_filename = path.join(settings.EXPORT_DIR, filename)
        wb = xlsxwriter.Workbook(full_filename)
        # The bag should be inside
        from task.mail_report import create_risk_obj_files
        create_risk_obj_files(rr, rst, wb)
        wb.close()
        self.resp({"url": path.join(settings.EXPORT_PREFIX, filename)})


class SQLRiskListHandler(PrivilegeReq):

    @classmethod
    def parsing_schema_dict(cls):
        """给接口用的schema，防止反复写"""
        return {
            "cmdb_id": scm_int,
            Optional("schema_name", default=None): scm_str,
            # Optional("risk_sql_rule_id", default=None): scm_dot_split_int,
            Optional("rule_name", default=None): scm_str,
            scm_optional("date_start", default=None): scm_date,
            scm_optional("date_end", default=None): scm_date_end,
            # Optional("rule_type", default="ALL"): scm_one_of_choices(
            #     ["ALL"] + ALL_RULE_TYPES_FOR_SQL_RULE),
            # Optional("enable_white_list", default=True):
            #     scm_bool,  # 需要注意这个字段的实际值，query_args时是0或1的字符，json时是bool
            # Optional("sort_by", default="sum"): scm_one_of_choices(["sum", "average"]),
            # Optional("severity", default=None): scm_dot_split_str,
            scm_optional("task_record_id"): scm_int
        }

    async def get(self):
        """风险SQL列表"""
        params = self.get_query_args(Schema({
            **self.parsing_schema_dict(),

            Optional("page", default=1): scm_int,
            Optional("per_page", default=10): scm_int,
        }))
        p = self.pop_p(params)
        date_range = params.pop("date_start"), params.pop("date_end")

        if "task_record_id" in params.keys():
            params["task_record_id_to_replace"] = {
                params["cmdb_id"]: params["task_record_id"]
            }

        with make_session() as session:
            try:
                rst = await AsyncTimeout(60).async_thr(
                    sql_utils.get_risk_sql_list,
                    session=session,
                    **params,
                    date_range=date_range
                )
            except NoRiskRuleSetException:
                self.resp(msg="未设置风险规则。")
                return
            rst_this_page, p = self.paginate(rst, **p)
        self.resp(rst_this_page, **p)


class SQLRiskRuleHandler(AuthReq):

    def get(self):
        """风险sql外层规则,违反规则个数等"""
        params = self.get_query_args(Schema({
            "cmdb_id": scm_int,
            Optional("schema"): scm_str,
            Optional("rule_name", default=None): scm_dot_split_str,
            Optional("severity"): scm_str,
            "date_start": scm_date,
            "date_end": scm_date_end,

            Optional("page", default=1): scm_int,
            Optional("per_page", default=10): scm_int,
        }))
        p = self.pop_p(params)
        date_start = params.pop("date_start")
        date_end = params.pop("date_end")
        rule_name: Union[list, None] = params.pop("rule_name")

        risk_sql_rule = StatsRiskSqlRule.objects(**params).order_by("-etl_date")
        if date_start:
            risk_sql_rule = risk_sql_rule.filter(etl_date__gte=date_start)
        if date_end:
            risk_sql_rule = risk_sql_rule.filter(etl_date__lte=date_end)
        if rule_name:
            risk_sql_rule = risk_sql_rule.filter(rule__rule_name__in=rule_name)
        risk_sql_rule = [x.to_dict() for x in risk_sql_rule]
        rst_this_page, p = self.paginate(risk_sql_rule, **p)

        self.resp(rst_this_page, **p)


class SQLRiskExportReportHandler(AuthReq):

    async def post(self):
        """风险SQL列表导出v2"""
        params = self.get_json_args(Schema({
            "cmdb_id": scm_int,
            "date_start": scm_date,
            "date_end": scm_date_end,
            Optional("schema", default=None): scm_str,
            Optional("severity", default=None): list,
            Optional("rule_name", default=None): list,
            Optional("_id", default=None): list
        }))
        cmdb_id = params.pop("cmdb_id")
        schema: Union[str, None] = params.pop("schema")
        date_start, date_end = params.pop("date_start"), params.pop("date_end")
        rule_name: Union[list, None] = params.pop("rule_name")
        severity: Union[list, None] = params.pop("severity")
        ids: Union[list, None] = params.pop("_id")
        del params

        rr, rst = await async_thr(risk_sql_export_data, cmdb_id=cmdb_id, schema=schema,
                                  date_start=date_start, date_end=date_end,
                                  severity=severity, rule_name=rule_name,
                                  ids=ids)

        filename = f"risk_sql_risk_{arrow.now().format(COMMON_DATETIME_FORMAT)}.xlsx"
        full_filename = path.join(settings.EXPORT_DIR, filename)
        wb = xlsxwriter.Workbook(full_filename)
        # The bag should be inside
        from task.mail_report import create_risk_sql_files
        create_risk_sql_files(rr, rst, wb)
        wb.close()
        self.resp({"url": path.join(settings.EXPORT_PREFIX, filename)})


class SQLRiskDetailHandler(AuthReq):

    @timing()
    async def get(self):
        """风险详情（include sql text, sql plan and statistics）"""
        params = self.get_query_args(Schema({
            "cmdb_id": scm_int,
            "sql_id": scm_str,
            Optional("risk_sql_rule_id", default=None): scm_dot_split_int,
            Optional("date_start", default=arrow.now().shift(months=-1).date()): scm_date,
            Optional("date_end", default=arrow.now().shift(days=1).date()): scm_date_end,
        }))
        cmdb_id = params.pop("cmdb_id")
        sql_id = params.pop("sql_id")
        risk_rule_id_list: list = params.pop("risk_sql_rule_id")
        date_start = params.pop("date_start")
        date_end = params.pop("date_end")
        del params  # shouldn't use params anymore

        with make_session() as session:
            db_model = session.query(CMDB.db_model).filter(CMDB.cmdb_id == cmdb_id)[0][0]
            risk_rules = session.query(RiskSQLRule).filter(RiskSQLRule.db_model == db_model)
            if risk_rule_id_list:
                # 给出了risk sql id就好办
                risk_rules = risk_rules.filter(
                    RiskSQLRule.risk_sql_rule_id.in_(risk_rule_id_list))
            else:
                rule_names = await AsyncTimeout(10).async_thr(
                    rule_utils.get_all_risk_towards_a_sql,
                    session=session,
                    sql_id=sql_id,
                    date_range=(date_start, date_end)
                )
                risk_rules = risk_rules.filter(RiskSQLRule.rule_name.in_(rule_names))

            sql_text_stats = await async_thr(
                sql_utils.get_sql_id_stats, session, cmdb_id=cmdb_id)
            latest_sql_text_object = SQLText.objects(sql_id=sql_id). \
                order_by("-etl_date"). \
                first()
            if not latest_sql_text_object:
                self.resp_not_found(msg="不存在该SQL")
                return
            schemas = list(set(MSQLPlan.objects(sql_id=sql_id, cmdb_id=cmdb_id).
                               distinct("schema")))

            hash_values = set(MSQLPlan.objects(sql_id=sql_id, cmdb_id=cmdb_id).
                              distinct("plan_hash_value"))
            sql_plan_stats = sql_utils.get_sql_plan_stats(session, cmdb_id)
            plans = [
                # {check codes blow for structure details}
            ]
            graphs = {
                plan_hash_value: {
                    # 总数
                    'cpu_time_delta': defaultdict(list),
                    'disk_reads_delta': defaultdict(list),
                    'elapsed_time_delta': defaultdict(list),
                    'buffer_gets_delta': defaultdict(list),  # 逻辑读

                    # 平均数
                    'cpu_time_average': defaultdict(list),
                    'disk_reads_average': defaultdict(list),
                    'elapsed_time_average': defaultdict(list),
                    'buffer_gets_average': defaultdict(list),
                } for plan_hash_value in hash_values
            }

            # 全部plan数据以及stats数据(后续需要出平均值)
            sql_stats = {
                "executions_delta": [],
                "io_cost": [],
                "elapsed_time_delta": [],
            }

            for plan_hash_value in hash_values:
                # plans
                sql_plan_object = MSQLPlan.objects(
                    cmdb_id=cmdb_id,
                    sql_id=sql_id,
                    plan_hash_value=plan_hash_value
                ).first()
                sql_stats["io_cost"].append(sql_plan_object.io_cost)
                first_appearance = sql_plan_stats.get((sql_id, plan_hash_value), {}). \
                    get("first_appearance", None)
                last_appearance = sql_plan_stats.get((sql_id, plan_hash_value), {}). \
                    get("last_appearance", None)

                # plans 挪到下面去了

                # stats
                sql_stat_objects = SQLStat.objects(cmdb_id=cmdb_id, sql_id=sql_id,
                                                   plan_hash_value=plan_hash_value)
                if date_start:
                    sql_stat_objects = sql_stat_objects.filter(etl_date__gte=date_start)
                if date_end:
                    sql_stat_objects = sql_stat_objects.filter(etl_date__lte=date_end)
                sql_stats["elapsed_time_delta"] += \
                    list(sql_stat_objects.values_list("elapsed_time_delta"))
                sql_stats["executions_delta"] += \
                    list(sql_stat_objects.values_list("executions_delta"))
                gp = graphs[plan_hash_value]
                for sql_stat_obj in sql_stat_objects:
                    etl_date = sql_stat_obj.etl_date
                    # 总数
                    gp['cpu_time_delta'][str(sql_stat_obj.plan_hash_value)].append({
                        "date": etl_date,
                        "value": round(sql_stat_obj.cpu_time_delta, 2)
                    })
                    gp['elapsed_time_delta'][str(sql_stat_obj.plan_hash_value)].append({
                        "date": etl_date,
                        "value": round(sql_stat_obj.elapsed_time_delta, 2)
                    })
                    gp['disk_reads_delta'][str(sql_stat_obj.plan_hash_value)].append({
                        "date": etl_date,
                        "value": round(sql_stat_obj.disk_reads_delta, 2)
                    })
                    gp['buffer_gets_delta'][str(sql_stat_obj.plan_hash_value)].append({
                        "date": etl_date,
                        "value": round(sql_stat_obj.buffer_gets_delta, 2)
                    })

                    get_delta_average = lambda x: x / sql_stat_obj.executions_delta \
                        if x > 0 and sql_stat_obj.executions_delta > 0 else 0
                    # 平均数
                    gp['cpu_time_average'][str(sql_stat_obj.plan_hash_value)].append({
                        "date": etl_date,
                        "value": round(get_delta_average(sql_stat_obj.cpu_time_delta), 2)
                    })
                    gp['elapsed_time_average'][str(sql_stat_obj.plan_hash_value)].append({
                        "date": etl_date,
                        "value": round(get_delta_average(sql_stat_obj.elapsed_time_delta), 2)
                    })
                    gp['disk_reads_average'][str(sql_stat_obj.plan_hash_value)].append({
                        "date": etl_date,
                        "value": round(get_delta_average(sql_stat_obj.disk_reads_delta), 2)
                    })
                    gp['buffer_gets_average'][str(sql_stat_obj.plan_hash_value)].append({
                        "date": etl_date,
                        "value": round(get_delta_average(sql_stat_obj.buffer_gets_delta), 2)
                    })

                # deduplicate datetime as date
                for i in gp.values():
                    for j in i.values():
                        deduplicated_items = self.dict_to_verbose_dict_in_list(
                            dict(self.list_of_dict_to_date_axis(j, "date", "value")))
                        j.clear()
                        j.extend(deduplicated_items)

                plans.append({
                    "plan_hash_value": plan_hash_value,
                    "cost": sql_plan_object.cost,
                    "first_appearance": dt_to_str(first_appearance),
                    "last_appearance": dt_to_str(last_appearance),

                    # 总数
                    'cpu_time_delta': gp["cup_time_delta"][-1],
                    'disk_reads_delta': gp["disk_reads_delta"][-1],
                    'elapsed_time_delta': gp["elapsed_time_delta"][-1],
                    'buffer_gets_delta': gp["buffer_gets_delta"][-1],  # 逻辑读

                    # 平均数
                    'cpu_time_average': gp["cpu_time_average"][-1],
                    'disk_reads_average': gp["disk_reads_average"][-1],
                    'elapsed_time_average': gp["elapsed_time_average"][-1],
                    'buffer_gets_average': gp["buffer_gets_average"][-1],
                })

            sql_stats = {k: sum([j for j in v if j]) / len(v) if len(v) else 0
                         for k, v in sql_stats.items()}

            the_first_appearance_time = sql_text_stats.get(sql_id, {}) \
                .get("first_appearance", None)
            the_last_appearance_time = sql_text_stats.get(sql_id, {}) \
                .get("last_appearance", None)
            if not the_first_appearance_time or not the_last_appearance_time:
                print(f"* warning: this sql with sql_id({sql_id}) may currently "
                      f"under capturing and has no statistic info")

            self.resp({
                'sql_id': sql_id,
                "stats": sql_stats,
                'schema': schemas[0] if schemas else None,
                'first_appearance': dt_to_str(the_first_appearance_time),
                'last_appearance': dt_to_str(the_last_appearance_time),
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
        latest_plan = plans.first()  # 取出最后一次采集出来的record_id
        record_id = latest_plan.record_id

        filtered_plans = ["index", "operation_display", "options","object_name", "cardinality",
                          "bytes","cost", "time"]
        page_plans = ["ID", "Operation", "Name",
                      "Rows","Bytes","Cost (%CPU)", "Time"]
        plans = plans.filter(record_id=record_id). \
            values_list(*filtered_plans)

        pt = PrettyTable(page_plans)
        pt.align = "l"
        for p in plans:
            to_add = list(p)
            m, s = divmod(to_add[-1] if to_add[-1] else 0, 60)
            h, m = divmod(m, 60)
            to_add[-1] = "%02d:%02d:%02d" % (h, m, s)
            to_add[1] = to_add[1] + " " + to_add[2] if to_add[2] else to_add[1]
            to_add.pop(2)
            to_add = [i if i is not None else " " for i in to_add]
            if 8 > len(str(to_add[-4])) > 5:
                to_add[-4] = str(round(to_add[-4] // 1024)) + "K"
                if len(str(to_add[-4])) >= 8:
                    to_add[-4] = str(round(to_add[-4] // 1024 // 1024)) + "M"
            if 8 > len(str(to_add[-3])) > 5:
                to_add[-3] = str(round(to_add[-3] // 1024)) + "K"
                if len(str(to_add[-3])) >= 8:
                    to_add[-3] = str(round(to_add[-3] // 1024 // 1024)) + "M"
            if 8 > len(str(to_add[-2])) > 5:
                to_add[-2] = str(round(to_add[-2] // 1024)) + "K"
                if len(str(to_add[-2])) >= 8:
                    to_add[-2] = str(round(to_add[-2] // 1024 // 1024)) + "M"
            pt.add_row(to_add)
        output_table = str(pt)

        self.resp(output_table)


class TableInfoHandler(AuthReq):

    def get(self):
        params = self.get_query_args(Schema({
            "cmdb_id": scm_int,
            "schema_name": scm_unempty_str,
            "table_name": scm_unempty_str
        }))
        if params.get("table_name", None) and "." in params["table_name"]:
            print(f"warning: the original table_name is {params['table_name']} "
                  f"the word before the dot is recognized as schema and has been ignored.")
            params["table_name"] = params["table_name"].split(".")[1]
        latest_tab_info = ObjTabInfo.objects(table_name=params["table_name"]). \
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


class OverviewHandler(PrivilegeReq):

    async def get(self):
        """数据库健康度概览"""
        self.acquire(PRIVILEGE.PRIVILEGE_ONLINE)

        params = self.get_query_args(Schema({
            "cmdb_id": scm_int,
            Optional("period", default=StatsCMDBLoginUser.DATE_PERIOD[0]):
                And(scm_int, scm_one_of_choices(StatsCMDBLoginUser.DATE_PERIOD))
        }))
        cmdb_id = params.pop("cmdb_id")
        period = params.pop("period")
        del params  # shouldn't use params anymore

        with make_session() as session:
            # physical size of current CMDB
            latest_task_record = await async_thr(
                score_utils.get_latest_task_record_id, session, cmdb_id)
            latest_task_record_id = latest_task_record.get(cmdb_id, None)
            if not latest_task_record:
                return self.resp_bad_req(msg=f"当前库未采集或者没有采集成功。")

        tablespace_sum = {}
        stats_phy_size_object = StatsCMDBPhySize.objects(
            task_record_id=latest_task_record_id, cmdb_id=cmdb_id).first()
        if stats_phy_size_object:
            tablespace_sum = stats_phy_size_object.to_dict(
                iter_if=lambda k, v: k in ("total", "used", "usage_ratio", "free"),
                iter_by=lambda k, v: round(v, 2) if k in ("usage_ratio",) else v)

        stats_cmdb_obj = StatsCMDBLoginUser.objects(
            login_user=self.current_user,
            cmdb_id=cmdb_id,
            task_record_id=latest_task_record_id,
            date_period=period
        ).first()
        if not stats_cmdb_obj:
            return self.resp_bad_req(msg="当前库未采集，请采集后重试。")

        self.resp({
            # 以下是按照给定的时间区间搜索的结果
            **stats_cmdb_obj.to_dict(),

            # 以下是取最近一次扫描的结果
            "tablespace_sum": tablespace_sum,
        })


class OverviewScoreByHandler(AuthReq):

    async def get(self):
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
            ret = await async_thr(
                score_utils.calc_score_by, session, cmdb, perspective, score_type)
            if perspective == OVERVIEW_ITEM_SCHEMA:
                d = sorted(
                    self.dict_to_verbose_dict_in_list(ret, "schema", "num"),
                    key=lambda k: k["num"]
                )[:10]
            elif perspective == OVERVIEW_ITEM_RADAR:
                d = ret
            else:
                assert 0
            self.resp({
                "data": d,
                "score_type": score_type
            })


class TablespaceListHandler(AuthReq):

    async def get(self):
        """表空间列表，以及最后一次采集到的使用情况"""
        params = self.get_query_args(Schema({
            "cmdb_id": scm_gt0_int,
            **self.gen_p()
        }))
        p = self.pop_p(params)
        cmdb_id = params.pop("cmdb_id")
        with make_session() as session:
            try:
                latest_task_record_ids = await async_thr(
                    score_utils.get_latest_task_record_id,
                    session,
                    cmdb_id=cmdb_id
                )
                latest_task_record_id = latest_task_record_ids[cmdb_id]
            except:
                return self.resp_bad_req(msg="当前库没有采到表空间信息")
            ots_q = ObjTabSpace.objects(
                cmdb_id=cmdb_id, task_record_id=latest_task_record_id). \
                order_by("-usage_ratio")
            items, p = self.paginate(ots_q, **p)
            self.resp([i.to_dict() for i in items], **p)


class TablespaceHistoryHandler(AuthReq):

    def get(self):
        """某个表空间的使用率历史折线图"""
        params = self.get_query_args(Schema({
            "tablespace_name": scm_unempty_str,
            "cmdb_id": scm_int,
        }))
        ts_q = ObjTabSpace.objects(**params).order_by("-etl_date").limit(30)
        ret = self.list_of_dict_to_date_axis(
            [i.to_dict(datetime_to_str=False) for i in ts_q],
            "etl_date",
            "usage_ratio"
        )
        ret = self.dict_to_verbose_dict_in_list(dict(ret[-7:]))
        self.resp(ret)


class TablespaceSumHistoryHandler(AuthReq):

    def get(self):
        """总表空间大小历史折线图"""
        params = self.get_query_args(Schema({
            "cmdb_id": scm_int,
        }))
        spz_q = StatsCMDBPhySize.objects(**params).order_by("-etl_date").limit(30)
        ret = self.list_of_dict_to_date_axis(
            [i.to_dict(datetime_to_str=False) for i in spz_q],
            "etl_date",
            "usage_ratio"
        )
        ret = self.dict_to_verbose_dict_in_list(dict(ret[-7:]))
        self.resp(ret)
