from os import path
from collections import Counter, defaultdict

import settings
from rule.const import *
from .base import OraclePrivilegeReq
from ..issue import OracleOnlineIssue
from ..tasks.schema_report_export import SchemaReportExport
from ..tasks.capture.cmdb_task_capture import OracleCMDBTaskCapture
from ..statistics import OracleStatsSchemaScore
from ..capture import OracleSQLText, OracleSQLStatToday, OracleSQLPlanToday
from rule.rule_cartridge import RuleCartridge
from auth.const import *
from utils.schema_utils import *
from utils.datetime_utils import *
from restful_api.modules import *
from models.sqlalchemy import *


@as_view("schema", group="health-center")
class HealthCenterSchema(OraclePrivilegeReq):

    def get(self):
        """健康中心schema分数列表,拿取每天最后一次采集情况
        前端建议展示:connect_name,schema_name,
            entry_score的sqlplan、sqlstat、sqltext、object、sql，
            create_time"""
        self.acquire(PRIVILEGE.PRIVILEGE_HEALTH_CENTER)

        params = self.get_query_args(Schema({
            scm_optional("connect_name", default=None): scm_unempty_str,
            scm_optional("schema_name"): scm_unempty_str,
            scm_optional("add_to_rate", default=True): scm_bool,
            "date_start": scm_date,
            "date_end": scm_date_end,
            **self.gen_p(),
        }))
        p = self.pop_p(params)
        connect_name = params.pop("connect_name")
        date_start, date_end = params.pop("date_start"), params.pop("date_end")

        with make_session() as session:
            cmdbs_tasks = session.query(OracleCMDBTaskCapture)

            connect_date_lastest_task_record = {}
            connect_cmdb_id = {}
            cmdb_id_connect = {}
            date_lastest_task_record_collection = []  # 每一个库每天最后的任务id

            for cmdb_task in cmdbs_tasks:
                date_latest_task_record = cmdb_task.day_last_succeed_task_record_id(
                    date_start=date_start,
                    date_end=date_end
                )
                connect_date_lastest_task_record[cmdb_task.connect_name] = list(date_latest_task_record.values())
                connect_cmdb_id[cmdb_task.connect_name] = cmdb_task.cmdb_id
                cmdb_id_connect[cmdb_task.cmdb_id] = cmdb_task.connect_name
                date_lastest_task_record_collection.extend(list(date_latest_task_record.values()))

            schema_score_q = OracleStatsSchemaScore.filter(**params)
            if connect_name:
                schema_score_q = schema_score_q.filter(cmdb_id=connect_cmdb_id[connect_name],
                                                       task_record_id__in=connect_date_lastest_task_record[
                                                           connect_name])
            else:
                schema_score_q = schema_score_q.filter(**params). \
                    filter(task_record_id__in=list(set(date_lastest_task_record_collection)))
            schema_score_q = schema_score_q.order_by("-create_time")
            schema_score = []
            for s_s in schema_score_q:
                s_s = s_s.to_dict()
                s_s['connect_name'] = cmdb_id_connect[s_s['cmdb_id']]
                schema_score.append(s_s)
            ret, p = self.paginate(schema_score, **p)
            self.resp(ret, **p)

    get.argument = {
        "querystring": {
            "//connect_name": "to_sqlaudit  ",
            "//schema_name": "ISQLAUDIT_DEV",
            "//add_to_rate": "1",
            "date_start": "2020-05-15",
            "date_end": "2020-05-20",
            "//page": "1",
            "//per_page": "10"
        }}


@as_view("rule_issue", group="health-center")
class HealthCenterSchemaIssueRule(OraclePrivilegeReq):

    def schema_issue_rule(self):

        params = self.get_query_args(Schema({
            "cmdb_id": scm_int,
            "task_record_id": scm_int,
            "schema_name": scm_unempty_str,
            scm_optional("level",default=None): scm_int
        }))
        cmdb_id = params.pop("cmdb_id")
        schema_name = params.pop("schema_name")
        task_record_id = params.pop("task_record_id")
        level = params.pop("level")

        issues_rule_q = OracleOnlineIssue.filter(
            cmdb_id=cmdb_id,
            schema_name=schema_name,
            task_record_id=task_record_id,
        )  # ?entries
        if level:
            issues_rule_q = issues_rule_q.filter(level=level)
        rule_issues = []
        levels = []
        create_time = ""
        dt = defaultdict(lambda: {"issue_num": 0})

        for issue_rule in issues_rule_q:
            create_time = issue_rule.create_time
            levels.append(issue_rule.level)
            doc = dt[issue_rule.rule_name]
            doc['rule_name'] = issue_rule.rule_name
            doc['rule_desc'] = issue_rule.rule_desc
            doc['level'] = issue_rule.level
            doc['issue_num'] += 1
        for d in dt.values():
            rule_issues.append(d)

        level_num = {RULE_LEVELS_CHINESE[RULE_LEVEL_INFO]: 0, RULE_LEVELS_CHINESE[RULE_LEVEL_WARNING]: 0,
                     RULE_LEVELS_CHINESE[RULE_LEVEL_SEVERE]: 0}
        level_num_c = Counter(levels)
        level_num[RULE_LEVELS_CHINESE[RULE_LEVEL_INFO]] = level_num_c.get(RULE_LEVEL_INFO, 0)
        level_num[RULE_LEVELS_CHINESE[RULE_LEVEL_WARNING]] = level_num_c.get(RULE_LEVEL_WARNING, 0)
        level_num[RULE_LEVELS_CHINESE[RULE_LEVEL_SEVERE]] = level_num_c.get(RULE_LEVEL_SEVERE, 0)

        schema_score = OracleStatsSchemaScore.filter(cmdb_id=cmdb_id,
                                                     schema_name=schema_name,
                                                     task_record_id=task_record_id).first()
        with make_session() as session:
            the_cmdb = self.cmdbs(session).filter_by(cmdb_id=cmdb_id).first()
            schema_issue_rule_dict = {"connect_name": the_cmdb.connect_name,
                                      "schema_name": schema_name,
                                      "create_time": dt_to_str(create_time),
                                      "schema_score": schema_score.to_dict(),
                                      **level_num,
                                      "rule_issue": rule_issues}
            return schema_issue_rule_dict, cmdb_id, task_record_id, schema_name

    def get(self):
        """健康中心schema触犯的规则,分数"""
        scheam_issue_rule, _, _, _ = self.schema_issue_rule()
        self.resp(scheam_issue_rule)

    get.argument = {
        "querystring": {
            "cmdb_id": "13",
            "schema_name": "ISQLAUDIT_DEV",
            "task_record_id": "27",
            "//level": "2"
        }
    }


@as_view("rule_issue_output", group="health-center")
class HealthCenterIssueRuleOutput(OraclePrivilegeReq):

    def get(self):
        """查询一次采集一个库一个schema触犯一个规则的输出参数,
        结果字段是动态由后端返回"""
        params = self.get_query_args(Schema({
            "cmdb_id": scm_int,
            "schema_name": scm_unempty_str,
            "task_record_id": scm_int,
            "rule_name": scm_unempty_str,
            **self.gen_p()
        }))
        cmdb_id = params.pop("cmdb_id")
        schema_name = params.pop("schema_name")
        task_record_id = params.pop("task_record_id")
        rule_name = params.pop("rule_name")
        p = self.pop_p(params)

        issues_q = OracleOnlineIssue.filter(cmdb_id=cmdb_id,
                                            schema_name=schema_name,
                                            task_record_id=task_record_id,
                                            rule_name=rule_name)
        field = []
        output_data = []

        rule_cartridge = RuleCartridge.filter(name=rule_name).first()
        rule_summary = rule_cartridge.summary
        rule_solution = rule_cartridge.solution

        for issue in issues_q:
            issue.output_params._data.pop("_cls")
            output_data.append(issue.output_params._data)
            field = list(issue.output_params._data.keys())

        output_data, p = self.paginate(output_data, **p)
        self.resp({"field": field,
                   "output_data": output_data,
                   "rule_summary": rule_summary,
                   "rule_solution": rule_solution,
                   "cmdb_id": cmdb_id,
                   "task_record_id": task_record_id,
                   "schema_name": schema_name}, **p)

    get.argument = {
        "querystring": {
            "cmdb_id": "2526",
            "schema_name": "ISQLAUDIT_DEV",
            "task_record_id": "47",
            # "rule_name": "TABLE_MIS_PK",
            # "rule_name": "SEQ_CACHESIZE",
            "rule_name": "SQL_LOOP_NUM",
            "//page": "1",
            "//per_page": "10"
        }}


@as_view("sql_issue_detail", group="health-center")
class SQLIssueDetailHandler(OraclePrivilegeReq):

    def get(self):
        """健康中心问题SQL问题下钻到"规则概览"的后续接口
        sqltext只传递sql_id且返回数据中不会有执行计划和执行特征，因为属于文本检查,
        OBJ的表信息的不在有这层下转(也就是没有sql_id的,数据为表的,前端就不在下转了)"""

        params = self.get_query_args(Schema({
            "task_record_id": scm_int,
            "sql_id": scm_unempty_str,
            scm_optional("schema_name"): scm_unempty_str,
            scm_optional("cmdb_id"): scm_int,
            scm_optional("plan_hash_value", default=None): scm_int
        }))
        task_record_id = params.pop("task_record_id")
        sql_id = params.pop("sql_id")
        plan_hash_value = params.pop("plan_hash_value")
        del params
        sql_text_object = OracleSQLText.filter(
            task_record_id=task_record_id,
            sql_id=sql_id
        ).first()
        plan_text = ""
        stat_dict = {}
        if plan_hash_value:
            plan_text = OracleSQLPlanToday.sql_plan_table(
                task_record_id=task_record_id,
                sql_id=sql_id,
                plan_hash_value=plan_hash_value
            )
            stat_object = OracleSQLStatToday.filter(
                task_record_id=task_record_id,
                sql_id=sql_id,
                plan_hash_value=plan_hash_value
            ).first()
            if stat_object:
                stat_dict = stat_object.to_dict()
        self.resp({
            "sql_text": sql_text_object.longer_sql_text,
            "sql_plan": plan_text,
            "sql_stat": stat_dict
        })

    get.argument = {
        "querystring": {
            "//cmdb_id": "2526",
            "//schema_name": "ISQLAUDIT_DEV",
            "task_record_id": "47",
            "sql_id": "2h37v66c9spu6",
            "//plan_hash_value": "2959612647"
        }}


@as_view("scheam_report_export", group="health-center")
class HealthCenterSchemaReportExport(HealthCenterSchemaIssueRule):

    async def get(self):
        """健康中心schema报告导出"""

        schema_issue_rule_dict, cmdb_id, task_record_id, schema_name = self.schema_issue_rule()  # 一个库一个schema一次采集触犯的所有规则

        issues_q = OracleOnlineIssue.filter(cmdb_id=cmdb_id,
                                            schema_name=schema_name,
                                            task_record_id=task_record_id)
        output_data = []
        for issue in issues_q:
            issue.output_params._data.pop("_cls")
            output_data.append({issue.rule_name: issue.output_params._data})

        parame_dict = {
            "schema_issue_rule_dict": schema_issue_rule_dict,
            "output_data": output_data
        }

        filename = f"export_schema_report_{dt_to_str(arrow.now())}.xlsx"

        await SchemaReportExport.async_shoot(filename=filename, parame_dict=parame_dict)
        await self.resp({"url": path.join(settings.EXPORT_PREFIX_HEALTH, filename)})

    get.argument = {
        "querystring": {
            "cmdb_id": "13",
            "schema_name": "ISQLAUDIT_DEV",
            "task_record_id": "10",
        }
    }
