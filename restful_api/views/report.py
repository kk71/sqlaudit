# Author: kk.Fang(fkfkbill@gmail.com)

from os import path

import arrow
import xlsxwriter
from schema import Optional, Schema, And
from functools import reduce
from collections import defaultdict

import os
import re
import time
import settings
from utils.const import *
from utils.datetime_utils import *
from utils.schema_utils import *
from utils import score_utils, const
from .base import AuthReq, PrivilegeReq
from models.mongo import *
from models.oracle import *
from utils import cmdb_utils
from utils.conc_utils import *
from task.mail_report import zip_file_path

from html_report import cmdb_export
import html_report.export


class OnlineReportTaskOuterHandler(PrivilegeReq):

    async def get(self):
        """在线查看报告任务外层列表"""

        self.acquire(const.PRIVILEGE.PRIVILEGE_HEALTH_CENTER)

        params = self.get_query_args(Schema({
            Optional("cmdb_id"): scm_int,
            Optional("connect_name"): scm_unempty_str,
            Optional("schema_name", default=None): scm_unempty_str,
            "status": And(scm_int, scm_one_of_choices(const.ALL_JOB_STATUS)),
            "date_start": scm_date,
            "date_end": scm_date_end,
            **self.gen_p(),
        }))
        p = self.pop_p(params)
        schema_name = params.pop("schema_name")
        date_start, date_end = params.pop("date_start"), params.pop("date_end")

        with make_session() as session:
            cmdb_ids = await async_thr(
                cmdb_utils.get_current_cmdb, session, self.current_user)

        job_q = Job.objects(score__nin=[None, 0],
                            cmdb_id__in=cmdb_ids,
                            **params).order_by("-create_time")
        if schema_name:
            job_q = job_q.filter(desc__owner=schema_name)
        if date_start:
            job_q = job_q.filter(create_time__gte=date_start)
        if date_end:
            job_q = job_q.filter(create_time__lte=date_end)

        ds = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: defaultdict())))
        for j in job_q:
            doc = ds[j.connect_name][j.desc.owner][j.record_id]
            doc['connect_name'] = j.connect_name
            doc['schema_name'] = j.desc.owner
            doc['record_id'] = j.record_id
            doc['create_time'] = arrow.get(j.create_time).format(COMMON_DATE_FORMAT)
            doc['status'] = j.status
        ret = []
        for x in ds.values():
            for y in x.values():
                for z in y.values():
                    ret.append(dict(z))
        for x in ret:
            x['score_min'] = []
            for y in job_q:
                if x['record_id'] == y.record_id:
                    x['score_min'].append(y.score)
            x['score_min'] = min(x['score_min'])
        rets = sorted(ret, key=lambda x: x['create_time'], reverse=True)
        rets, p = self.paginate(rets, **p)
        self.resp(rets, **p)


class OnlineReportDimensionHandler(AuthReq):

    def get(self):
        """在线查看报告 库用户时间下四个维度"""
        params = self.get_query_args(Schema({
            "connect_name": scm_unempty_str,
            "schema_name": scm_unempty_str,
            "record_id": scm_unempty_str,  # ##需传递%23%23十六进制值

        }))
        schema_name = params.pop("schema_name")
        job_q = Job.objects(score__nin=[None, 0],
                            desc__owner=schema_name, **params)
        job = [x.to_dict() for x in job_q]
        self.resp(job)


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
            job = Job.objects(id=job_id).first()
            result = Results.objects(task_uuid=job_id).first()
            cmdb = session.query(CMDB).filter_by(cmdb_id=result.cmdb_id).first()
            if not cmdb:
                self.resp_not_found(msg="纳管数据库不存在")
                return
            rules_violated, score_sum = await async_thr(
                score_utils.calc_result, result, cmdb.db_model, obj_type_info)
            self.resp({
                "job_id": job_id,
                "cmdb": cmdb.to_dict(),
                "rules_violated": rules_violated,
                "score_sum": score_sum,
                "schema": job.desc["owner"],
                **result.to_dict(iter_if=lambda k, v: k in ("create_date",))
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
            plans = [i.to_dict(iter_if=lambda k, v: k in (
                "operation",
                "options",
                "object_owner",
                "object_name",
                "cost",
                "cardinality",
                "operation_display"
            )) for i in dict(sorted(plans)).values()]

        self.resp({
            "sql_text": sql.sql_text,
            "sql_plan": plans,
            "execution_stat": execution_stat
        })


class ExportReportXLSXHandler(AuthReq):

    async def get(self):
        """导出报告为xlsx"""
        params = self.get_query_args(Schema({
            "job_id": scm_str_to_list,
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
            job_info = Job.objects(id=job_id).first()

            port = 1521 if str(job_info.desc.port) == "1521" else int(job_info.desc.port)
            search_temp = {
                "db_ip": job_info.desc.db_ip,
                "owner": job_info.desc.owner
            }
            if port == 1521:
                search_temp["instance_name"] = job_info.desc.instance_name or "空"

            with make_session() as session:
                cmdb = session.query(CMDB).filter_by(cmdb_id=result.cmdb_id).first()
                rules_violated, score_sum = await async_thr(
                    score_utils.calc_result, result, cmdb.db_model)
                rules_violateds = []
                for x in rules_violated:
                    rules_violateds.append([x['rule']['rule_name'],
                                            x['rule']['rule_desc'],
                                            x['violated_num'],
                                            x['deduction'],
                                            x['weighted_deduction']])

                heads = ['任务ID', 'IP地址', '端口号', 'SCHEMA用户', '规则类型', '最终得分']
                heads_data = [job_id, search_temp["db_ip"], port,
                              search_temp["owner"], job_info.name.split('#')[1], score_sum]

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

                filename = f"export_sqlhealth_details_{job_info.name.split('#')[1]}_{arrow.now().format('YYYY-MM-DD-HH-mm-ss')}-{a}.xlsx"
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
            "job_id": scm_str_to_list,
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

            sql_time_num = ret_w['sql_execution_cost_rank']['by_sum']

            risk_rule_rank = ret_w['risk_rule_rank']

            ret_schema_avg = score_utils.calc_score_by(session, cmdb_q, perspective_schema, score_type_avg)
            ret_schema_min = score_utils.calc_score_by(session, cmdb_q, perspective_schema, score_type_min)
            user_health_ranking_avg = sorted(
                self.dict_to_verbose_dict_in_list(ret_schema_avg, "schema", "num"),
                key=lambda k: k["num"])
            user_health_ranking_min = sorted(
                self.dict_to_verbose_dict_in_list(ret_schema_min, "schema", "num"),
                key=lambda k: k["num"])

            path = cmdb_export.cmdb_report_export_html(cmdb, cmdb_q, tablespace_sum,
                                                       radar_avg, radar_score_avg,
                                                       radar_min, radar_score_min,
                                                       tab_space_q,
                                                       active_week, at_risk_week,
                                                       active_mouth, at_risk_mouth,
                                                       sql_time_num,
                                                       risk_rule_rank,
                                                       user_health_ranking_avg,
                                                       user_health_ranking_min)
            self.resp({
                "url": path
            })
