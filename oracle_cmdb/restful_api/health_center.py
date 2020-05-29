
from collections import Counter,defaultdict

from .base import OraclePrivilegeReq
from ..cmdb import OracleCMDB
from ..issue.base import OracleOnlineIssue
from ..tasks.capture.cmdb_task_capture import OracleCMDBTaskCapture
from ..statistics.current_task.schema_score import OracleStatsSchemaScore
from rule.const import *
from rule.rule_cartridge import RuleCartridge
from auth.const import *
from utils.schema_utils import *
from utils.datetime_utils import dt_to_str
from restful_api.modules import as_view
from models.sqlalchemy import make_session


@as_view("schema", group="health_center")
class HealthCenterSchema(OraclePrivilegeReq):

    def get(self):
        """健康中心schema分数列表,拿取每天最后一次采集情况
        前端建议展示:connect_name,schema_name,entry_score的sqlplan、sqlstat、sqltext、object、sql，create_time"""
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
            "//add_to_rate": "True",
            "date_start": "2020-05-15",
            "date_end": "2020-05-20",
            "page": "1",
            "per_page": "10"
        }}


@as_view("rule_issue", group="health_center")
class HealthCenterSchemaIssueRule(OraclePrivilegeReq):

    def get(self):
        """健康中心schema触犯的规则,分数"""
        params = self.get_query_args(Schema({
            "cmdb_id" : scm_int,
            "task_record_id" : scm_int,
            "schema_name" : scm_unempty_str,
            **self.gen_p()
        }))
        p = self.pop_p(params)
        cmdb_id = params.pop("cmdb_id")
        schema_name = params.pop("schema_name")
        task_record_id = params.pop("task_record_id")

        issues_rule_q=OracleOnlineIssue.filter(cmdb_id=cmdb_id,
                                               schema_name=schema_name,
                                               task_record_id=task_record_id)#?entries
        rule_issues = []
        levels = []
        create_time = ""
        dt=defaultdict(lambda :{"issue_num":0})

        for issue_rule in issues_rule_q:
            create_time=issue_rule.create_time
            levels.append(issue_rule.level)
            doc=dt[issue_rule.rule_name]
            doc['rule_name'] = issue_rule.rule_name
            doc['rule_desc'] = issue_rule.rule_desc
            doc['level'] = issue_rule.level
            doc['issue_num'] +=1
        for d in dt.values():
            rule_issues.append(d)

        level_num = {RULE_LEVEL_INFO: 0, RULE_LEVEL_WARNING: 0, RULE_LEVEL_SEVERE : 0}
        level_num_c=Counter(levels)
        level_num[RULE_LEVEL_INFO] = level_num_c.get(RULE_LEVEL_INFO, 0)
        level_num[RULE_LEVEL_WARNING] = level_num_c.get(RULE_LEVEL_WARNING, 0)
        level_num[RULE_LEVEL_SEVERE] = level_num_c.get(RULE_LEVEL_SEVERE , 0)

        schema_score=OracleStatsSchemaScore.filter(cmdb_id=cmdb_id,
                                      schema_name=schema_name,
                                      task_record_id=task_record_id).first()
        with make_session() as session:
            cmdb_q=session.query(OracleCMDB).filter_by(cmdb_id=cmdb_id).first()
            rule_issues,p=self.paginate(rule_issues,**p)
            self.resp({"connect_name":cmdb_q.connect_name,
                       "schema_name": schema_name,
                       "create_time": dt_to_str(create_time),
                       "schema_score": schema_score.to_dict(),
                       **level_num,
                       "rule_issue":rule_issues},**p)

    get.argument = {
        "querystring": {
            "cmdb_id": "2526",
            "schema_name": "ISQLAUDIT_DEV",
            "task_record_id": "32",
            "page": "1",
            "per_page": "10"
        }}


@as_view("rule_issue_output", group="health_center")
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
        p=self.pop_p(params)

        issues_q=OracleOnlineIssue.filter(cmdb_id=cmdb_id,
                                          schema_name=schema_name,
                                          task_record_id=task_record_id)
        field=[]
        output_data=[]

        rule_cartridge=RuleCartridge.filter(name=rule_name).first()
        rule_summary=rule_cartridge.summary
        rule_solution=rule_cartridge.solution

        for issue in issues_q:
            issue.output_params._data.pop("_cls")
            output_data.append(issue.output_params._data)

            english_field= tuple(issue.output_params._data.keys())
            for rule_op in rule_cartridge.output_params:
                if rule_op['desc'] in field:
                    continue
                if rule_op['name'] in english_field:
                    field.append(rule_op['desc'])

        output_data,p=self.paginate(output_data,**p)
        self.resp({"field":field,
                   "output_data":output_data,
                   "rule_summary":rule_summary,
                   "rule_solution":rule_solution,
                   "cmdb_id":cmdb_id,
                   "task_record_id":task_record_id,
                   "schema_name":schema_name},**p)

    get.argument = {
        "querystring": {
            "cmdb_id": "2526",
            "schema_name": "ISQLAUDIT_DEV",
            "task_record_id": "27",
            # "rule_name": "TABLE_MIS_PK",
            # "rule_name": "SEQ_CACHESIZE",
            "rule_name": "SQL_LOOP_NUM",
            "page": "1",
            "per_page": "10"
        }}
