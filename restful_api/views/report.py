# Author: kk.Fang(fkfkbill@gmail.com)

import xlsxwriter
from schema import Optional, Schema
from prettytable import PrettyTable

import os
import re
import time
import settings
from utils.const import *
from utils.datetime_utils import *
from utils.schema_utils import *
from utils.rule_utils import get_all_risk_towards_a_sql
from utils.sql_utils import get_risk_sql_list, get_sql_plan_stats
from utils import score_utils, const
from .base import AuthReq, PrivilegeReq
from models.mongo import *
from models.oracle import *
from utils import cmdb_utils
from utils.conc_utils import *
from task.mail_report import zip_file_path

from html_report import cmdb_export
import html_report.export


class OnlineReportSchemaRate(PrivilegeReq):

    def get(self):
        """schema维度的报告列表"""
        self.acquire(const.PRIVILEGE.PRIVILEGE_HEALTH_CENTER)

        params = self.get_query_args(Schema({
            Optional("cmdb_id"): scm_int,
            Optional("connect_name"): scm_unempty_str,
            Optional("schema_name"): scm_unempty_str,
            Optional("add_to_rate", default=True): scm_bool,
            "date_start": scm_date,
            "date_end": scm_date_end,
            **self.gen_p(),
        }))
        p = self.pop_p(params)
        date_start, date_end = params.pop("date_start"), params.pop("date_end")
        q = StatsSchemaRate.objects(**params).order_by("-etl_date")
        if date_start:
            q = q.filter(etl_date__gt=date_start)
        if date_end:
            q = q.filter(etl_date__lte=date_end)
        ret, p = self.paginate(q, **p)
        self.resp([
            i.to_dict(iter_by=lambda k, v: dt_to_str(list(v.values()))
            if k in ("score_rule_type",) else dt_to_str(v))
            for i in ret
        ], **p)


class OnlineReportTaskHandler(AuthReq):

    async def get(self):
        """在线查看某个报告"""
        params = self.get_query_args(Schema({
            "job_id": scm_unempty_str,
            Optional("obj_type_info", default=None): scm_str
        }))
        job_id = params.pop("job_id")
        obj_type_info = params.pop("obj_type_info")
        del params  # shouldn't use params anymore

        if job_id.lower() == "null":
            return self.resp_bad_req(msg="无报告。")

        with make_session() as session:
            result = Results.objects(task_uuid=job_id).first()
            cmdb = session.query(CMDB).filter_by(cmdb_id=result.cmdb_id).first()
            if not cmdb:
                self.resp_not_found(msg="纳管数据库不存在")
                return
            rules_violated, score_sum, severity_num = await async_thr(
                score_utils.calc_result, result, cmdb.db_model, obj_type_info)
            self.resp({
                "job_id": job_id,
                "cmdb": cmdb.to_dict(),
                "rules_violated": rules_violated,
                "score_sum": score_sum,
                "schema": result.to_dict()["score"]["schema_name"],
                **result.to_dict(iter_if=lambda k, v: k in ("create_date",)),
                **severity_num
            })


class OnlineReportRuleDetailHandler(AuthReq):

    async def get(self):
        """在线查看报告的规则细节(obj返回一个列表，其余类型返回sql文本相关的几个列表)"""
        params = self.get_query_args(Schema({
            "job_id": scm_unempty_str,
            "rule_name": scm_unempty_str
        }))
        job_id = params.pop("job_id")
        rule_name = params.pop("rule_name")
        del params

        with make_session() as session:
            rst = Results.objects(task_uuid=job_id).first()
            if not rst:
                return self.resp_not_found(msg=f"result not found: {job_id}")
            ret = await async_thr(rst.deduplicate_output, session, job_id, rule_name)
            self.resp({
                "columns": ret["columns"],
                "records": ret["records"],
                "rule": ret["rule"].to_dict(iter_if=lambda k, v: k in (
                    "rule_desc", "rule_name", "rule_type", "solution"))
                if ret["rule"] else {},
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
        if result.rule_type in (const.RULE_TYPE_SQLPLAN, const.RULE_TYPE_SQLSTAT):
            for sql_dict in rule_dict_in_rst.get("sqls", []):
                if sql_dict and sql_dict["sql_id"] == sql_id:
                    execution_stat = sql_dict["stat"]
                    plan_hash_value = sql_dict["plan_hash_value"]
                    break
        if plan_hash_value:
            plans = MSQLPlan.get_plans(plan_hash_value=plan_hash_value, sql_id=sql_id).items()
            page_plans = ["ID", "Operation", "Name",
                          "Rows", "Bytes", "Cost (%CPU)", "Time"]
            plans = [i.to_dict(iter_if=lambda k, v: k in (
                "index",
                "operation_display",
                "options",
                "object_name",
                "cardinality",
                "bytes",
                "cost",
                "time"
            )) for i in dict(sorted(plans)).values()]
            pt = PrettyTable(page_plans)
            pt.align = "l"
            for x in plans:
                m, s = divmod(x['time'] if x['time'] else 0, 60)
                h, m = divmod(m, 60)
                x['time'] = "%02d:%02d:%02d" % (h, m, s)
                if x["options"] is not None:
                    x["operation_display"] = x["operation_display"] + " " + x["options"]
                x.pop("options")
                x = {k: "" if v is None else v for k, v in x.items()}
                if 8 > len(str(x['cardinality'])) > 5:
                    x['cardinality']=str(round(x['cardinality']//1024))+"K"
                    if len(str(x['cardinality'])) >= 8:
                        x['cardinality']=str(round(x['cardinality']//1024//1024))+"M"
                if 8 > len(str(x['bytes'])) > 5:
                    x['bytes'] = str(round(x['bytes'] // 1024)) + "K"
                    if len(str(x['bytes'])) >= 8:
                        x['bytes'] = str(round(x['bytes'] // 1024 // 1024)) + "M"
                if 8 > len(str(x['cost'])) > 5:
                    x['cost'] = str(round(x['cost'] // 1024)) + "K"
                    if len(str(x['cost'])) >= 8:
                        x['cost'] = str(round(x['cost'] // 1024 // 1024)) + "M"
                pt.add_row([x['index'], x['operation_display'],
                            x['object_name'], x['cardinality'],
                            x['bytes'], x['cost'], x['time']])
            plans = str(pt)

        self.resp({
            "sql_text": sql.sql_text,
            "sql_plan": plans,
            "execution_stat": execution_stat
        })


class ExportReportXLSXHandler(AuthReq):

    async def get(self):
        """导出报告为xlsx"""
        params = self.get_query_args(Schema({
            "job_id": scm_dot_split_str,
            Optional(object): object

        }))
        job_ids = params.pop("job_id")
        del params

        # The path to the generated file
        path = "/tmp/" + str(int(time.time()))
        if not os.path.exists(path):
            os.makedirs(path)

        a = 1  # 防止类型一样导出错误
        for job_id in job_ids:
            result = Results.objects(task_uuid=job_id).first()
            # job_info = Job.objects(id=job_id).first()
            with make_session() as session:
                cmdb = session.query(CMDB).filter_by(cmdb_id=result.cmdb_id).first()
                port = cmdb.port
                ip = cmdb.ip_address
                schema_name = result.score.get("schema_name")
                rule_type = result.score.get("rule_type")
                # if port == 1521:
                #     search_temp["instance_name"] = job_info.desc.instance_name or "空"

                rules_violated, score_sum,_ = await async_thr(
                    score_utils.calc_result, result, cmdb.db_model)
                rules_violateds = []
                for x in rules_violated:
                    rules_violateds.append([x['rule']['rule_name'],
                                            x['rule']['rule_desc'],
                                            x['violated_num'],
                                            x['deduction'],
                                            x['weighted_deduction']])

                heads = ['任务ID', 'IP地址', '端口号', 'SCHEMA用户', '规则类型', '最终得分']
                heads_data = [job_id, ip, port,
                              schema_name, rule_type, score_sum]

                rule_heads = ['规则名称', '规则描述', '违反次数', '扣分', '加权扣分']
                rule_data_lists = rules_violateds
                excel_data_dict = {}

                for rule_data in rule_data_lists:
                    rule_name = rule_data[0]
                    rst = Results.objects(task_uuid=job_id).first()
                    rule_detail_data = await async_thr(rst.deduplicate_output, session, job_id, rule_name)
                    rule_info = Rule.objects(rule_name=rule_name, db_model=cmdb.db_model,
                                             db_type=const.DB_ORACLE).first()
                    solution = ''.join(rule_info['solution'])
                    rule_detail_datas = []
                    rule_detail_title = []
                    for x in rule_detail_data['records']:
                        rule_detail_datas.append(x.values())
                        for y in x.keys():
                            if y not in rule_detail_title:
                                rule_detail_title.append(y)
                    excel_data_dict.update(
                        {
                            rule_name: {
                                "rule_heads": rule_heads,
                                "rule_data": rule_data,
                                "solution": solution,
                                "records": rule_detail_datas,
                                "table_title": rule_detail_title,
                            }
                        }
                    )

                filename = f"export_sqlhealth_details_{rule_type}_{arrow.now().format('YYYY-MM-DD-HH-mm-ss')}-{a}.xlsx"
                a += 1
                full_filename = path + "/" + filename
                wb = xlsxwriter.Workbook(full_filename)
                format_title = wb.add_format({
                    'bold': 1,
                    'size': 14,
                    'align': 'center',
                    'valign': 'vcenter',

                })
                format_text = wb.add_format({
                    'valign': 'vcenter',
                    'align': 'center',
                    'size': 14,
                    'text_wrap': True,
                })

                for rule_key, rule_value in excel_data_dict.items():
                    rule_heads = rule_value['rule_heads']
                    rule_data = rule_value['rule_data']
                    solution = rule_value['solution']
                    records = rule_value['records']
                    table_title = rule_value['table_title']

                    rule_ws = wb.add_worksheet(re.sub('[*%]', '', rule_key[:20]))

                    rule_ws.set_column(0, 0, 40)
                    rule_ws.set_column(1, 1, 110)
                    rule_ws.set_column(2, 2, 30)
                    rule_ws.set_column(3, 6, 30)

                    [rule_ws.write(0, x, field, format_title) for x, field in enumerate(heads)]
                    [rule_ws.write(1, x, field, format_text) for x, field in enumerate(heads_data)]
                    [rule_ws.write(3, x, field, format_title) for x, field in enumerate(rule_heads)]
                    [rule_ws.write(4, x, field, format_text) for x, field in enumerate(rule_data)]
                    [rule_ws.write(6, x, field, format_title) for x, field in enumerate(table_title)]

                    num = 1
                    for records_data in records:
                        [rule_ws.write(6 + num, x, str(field), format_text) for x, field in
                         enumerate(records_data)]
                        num += 1

                    last_num = 6 + len(records) + 2

                    last_data = ['修改意见: ', solution]
                    [rule_ws.write(last_num, x, field, format_title) for x, field in
                     enumerate(last_data)]
                wb.close()
        """
        packaging
        zip_file_path(
        The path to the generated file,
        The path to place the file,
        The name of the package file)"""
        file_path_list = [
            "export_sqlhealth_details",
            datetime.now().strftime("%Y%m%d%H%M%S") + ".zip"
        ]
        zipPath = zip_file_path(
            path, settings.HEALTH_DIR, ''.join(file_path_list))
        self.resp({"url": zipPath})


class ExportReportHTMLHandler(AuthReq):

    async def get(self):
        """导出报告为html"""
        params = self.get_query_args(Schema({
            "job_id": scm_dot_split_str,
            Optional(object): object
        }))
        job_ids = params.pop("job_id")

        zipPath = await AsyncTimeout(60).async_thr(
            html_report.export.export_task, job_ids)
        self.resp({
            "url": zipPath
        })


class ExportReportCmdbHTMLHandler(AuthReq):

    def get(self):
        """导出库的html报告"""
        params = self.get_query_args(Schema({
            "cmdb_id": scm_int
        }))
        cmdb_id = params.pop("cmdb_id")
        del params

        score_type_avg = SCORE_BY_AVERAGE
        score_type_min = SCORE_BY_LOWEST
        perspective_schema = OVERVIEW_ITEM_SCHEMA
        perspective_radar = OVERVIEW_ITEM_RADAR
        period_week = StatsCMDBLoginUser.DATE_PERIOD[0]
        period_mouth = StatsCMDBLoginUser.DATE_PERIOD[1]

        with make_session() as session:
            cmdb_q = session.query(CMDB).filter_by(cmdb_id=cmdb_id). \
                first()
            cmdb = cmdb_q.to_dict()
            latest_task_record_id = score_utils.get_latest_task_record_id(session, cmdb_id) \
                .get(cmdb_id, None)

            tablespace_sum = StatsCMDBPhySize.objects(task_record_id=latest_task_record_id, cmdb_id=cmdb_id). \
                first().to_dict()

            cmdb_score = cmdb_utils.get_latest_cmdb_score(session)[cmdb_id]

            ret_radar_avg = score_utils.calc_score_by(session, cmdb_q, perspective_radar, score_type_avg)
            radar_avg = [{"name": k, "max": 100} for k in ret_radar_avg.keys()]
            radar_score_avg = [round(x) for x in ret_radar_avg.values()]
            ret_radar_min = score_utils.calc_score_by(session, cmdb_q, perspective_radar, score_type_min)
            radar_min = [{"name": k, "max": 100} for k in ret_radar_min.keys()]
            radar_score_min = [round(x) for x in ret_radar_min.values()]

            tab_space_q = ObjTabSpace.objects(
                cmdb_id=cmdb_id, task_record_id=latest_task_record_id). \
                order_by("-usage_ratio")

            rets = StatsCMDBLoginUser.objects(
                login_user=self.current_user,
                cmdb_id=cmdb_id,
                task_record_id=latest_task_record_id)
            ret_w = rets.filter(date_period=period_week).first().to_dict()
            ret_m = rets.filter(date_period=period_mouth).first().to_dict()
            active_week = ret_w['sql_num']['active']
            at_risk_week = ret_w['sql_num']['at_risk']
            active_mouth = ret_m['sql_num']['active']
            at_risk_mouth = ret_m['sql_num']['at_risk']

            risk_rule_rank = ret_w['risk_rule_rank']

            date_start = arrow.now().shift(days=-period_week + 1).date()
            date_end = arrow.now().shift(days=1).date()
            sqls = get_risk_sql_list(
                session=session,
                cmdb_id=cmdb_id,
                date_range=(date_start, date_end))[:30]

            sql_time_num_rank = [{"sql_id": sql["sql_id"],
                                  "time": sql["execution_time_cost_sum"]}
                                 for sql in sqls]

            db_model = session.query(CMDB.db_model).filter(CMDB.cmdb_id == cmdb_id)[0][0]
            risk_rules_q = session.query(RiskSQLRule).filter(RiskSQLRule.db_model == db_model)
            sql_plan_stats = get_sql_plan_stats(session, cmdb_id)
            filtered_plans = ["index", "operation_display", "options", "object_name", "cardinality",
                              "bytes", "cost", "time"]
            page_plans = ["ID", "Operation", "Name",
                          "Rows", "Bytes", "Cost (%CPU)", "Time"]
            for sql in sqls:
                e_d = []
                io_c = []
                e_t_d = []
                rule_names = get_all_risk_towards_a_sql(
                    session=session, sql_id=sql['sql_id'], date_range=(date_start, date_end))
                risk_rules = risk_rules_q.filter(RiskSQLRule.rule_name.in_(rule_names))
                sql['risk_rules'] = [rr.to_dict() for rr in risk_rules]

                msqlplan_q = MSQLPlan.objects(sql_id=sql['sql_id'], cmdb_id=cmdb_id)
                schemas = list(set(msqlplan_q.distinct("schema")))

                sql['schema'] = schemas[0] if schemas else None

                hash_values = set(msqlplan_q.distinct("plan_hash_value"))

                # sql['graphs']={plan_hash_value:{
                #     # 总数
                #     'cpu_time_delta': defaultdict(list),
                #     'disk_reads_delta': defaultdict(list),
                #     'elapsed_time_delta': defaultdict(list),
                #     'buffer_gets_delta': defaultdict(list),  # 逻辑读
                #
                #     # 平均数
                #     'cpu_time_average': defaultdict(list),
                #     'disk_reads_average': defaultdict(list),
                #     'elapsed_time_average': defaultdict(list),
                #     'buffer_gets_average': defaultdict(list),
                #
                # }for plan_hash_value in hash_values}

                sql['plans'] = []

                for plan_hash_value in hash_values:
                    sql_plan_q = msqlplan_q.filter(plan_hash_value=plan_hash_value)

                    sql_plan_object = sql_plan_q.first()

                    sql_stat_objects = SQLStat.objects(cmdb_id=cmdb_id, sql_id=sql['sql_id'],
                                                       plan_hash_value=plan_hash_value). \
                        filter(etl_date__gte=date_start, etl_date__lte=date_end)
                    e_d += list(sql_stat_objects.values_list("executions_delta"))
                    io_c += list(sql_plan_object.io_cost) if sql_plan_object.io_cost else []
                    e_t_d += list(sql_stat_objects.values_list("elapsed_time_delta"))

                    first_appearance = sql_plan_stats.get((sql['sql_id'], plan_hash_value), {}). \
                        get("first_appearance", None)
                    last_appearance = sql_plan_stats.get((sql['sql_id'], plan_hash_value), {}). \
                        get("last_appearance", None)

                    plans = sql_plan_q.order_by("-etl_date")
                    record_id = plans.first().record_id
                    plans = plans.filter(record_id=record_id).values_list(*filtered_plans)

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
                    sqlplan_table = str(pt)

                    sql['plans'].append({
                        "plan_hash_value": plan_hash_value,
                        "cost": sql_plan_object.cost,
                        "first_appearance": dt_to_str(first_appearance),
                        "last_appearance": dt_to_str(last_appearance),
                        "sqlplan": sqlplan_table
                    })

                    # gp=sql['graphs'][plan_hash_value]##{"p":[{},{}]}
                    # for sql_stat_obj in sql_stat_objects:
                    #     etl_date=sql_stat_obj.etl_date
                    #     #总数
                    #     gp['cpu_time_delta'][str(sql_stat_obj.plan_hash_value)].append({
                    #         "date": etl_date,
                    #         "value": round(sql_stat_obj.cpu_time_delta,2)
                    #     })
                    #     gp['elapsed_time_delta'][str(sql_stat_obj.plan_hash_value)].append({
                    #         "date": etl_date,
                    #         "value": round(sql_stat_obj.elapsed_time_delta, 2)
                    #     })
                    #     gp['disk_reads_delta'][str(sql_stat_obj.plan_hash_value)].append({
                    #         "date": etl_date,
                    #         "value": round(sql_stat_obj.disk_reads_delta, 2)
                    #     })
                    #     gp['buffer_gets_delta'][str(sql_stat_obj.plan_hash_value)].append({
                    #         "date": etl_date,
                    #         "value": round(sql_stat_obj.buffer_gets_delta, 2)
                    #     })
                    #
                    #     get_delta_average = lambda x: x / sql_stat_obj.executions_delta \
                    #         if x > 0 and sql_stat_obj.executions_delta > 0 else 0
                    #     # 平均数
                    #     gp['cpu_time_average'][str(sql_stat_obj.plan_hash_value)].append({
                    #         "date": etl_date,
                    #         "value": round(get_delta_average(sql_stat_obj.cpu_time_delta), 2)
                    #     })
                    #     gp['elapsed_time_average'][str(sql_stat_obj.plan_hash_value)].append({
                    #         "date": etl_date,
                    #         "value": round(get_delta_average(sql_stat_obj.elapsed_time_delta), 2)
                    #     })
                    #     gp['disk_reads_average'][str(sql_stat_obj.plan_hash_value)].append({
                    #         "date": etl_date,
                    #         "value": round(get_delta_average(sql_stat_obj.disk_reads_delta), 2)
                    #     })
                    #     gp['buffer_gets_average'][str(sql_stat_obj.plan_hash_value)].append({
                    #         "date": etl_date,
                    #         "value": round(get_delta_average(sql_stat_obj.buffer_gets_delta), 2)
                    #     })
                    # # deduplicate datetime as date
                    # for i in gp.values():
                    #     for j in i.values():
                    #         deduplicated_items = self.dict_to_verbose_dict_in_list(
                    #             dict(self.list_of_dict_to_date_axis(j, "date", "value")))
                    #         j.clear()
                    #         j.extend(deduplicated_items)

                sql['stats'] = {
                    "executions_delta": sum(e_d) / len(e_d) if len(e_d) else 0,
                    "io_cost": sum(io_c) / len(io_c) if len(io_c) else 0,
                    "elapsed_time_delta": sum(e_t_d) / len(e_t_d) if len(e_t_d) else 0,
                }

            ret_schema_avg = score_utils.calc_score_by(session, cmdb_q, perspective_schema, score_type_avg)
            ret_schema_min = score_utils.calc_score_by(session, cmdb_q, perspective_schema, score_type_min)
            user_health_ranking_avg = sorted(
                self.dict_to_verbose_dict_in_list(ret_schema_avg, "schema", "num"),
                key=lambda k: k["num"])
            user_health_ranking_min = sorted(
                self.dict_to_verbose_dict_in_list(ret_schema_min, "schema", "num"),
                key=lambda k: k["num"])

            path = cmdb_export.cmdb_report_export_html(cmdb, cmdb_q, tablespace_sum,
                                                       cmdb_score,
                                                       radar_avg, radar_score_avg,
                                                       radar_min, radar_score_min,
                                                       tab_space_q,
                                                       active_week, at_risk_week,
                                                       active_mouth, at_risk_mouth,
                                                       sql_time_num_rank,
                                                       risk_rule_rank,
                                                       user_health_ranking_avg,
                                                       user_health_ranking_min,
                                                       sqls)

            self.resp({
                "url": path
            })
