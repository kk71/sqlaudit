from typing import Union

from .base import OraclePrivilegeReq
from ..issue.sql import OracleOnlineSQLIssue
from ..capture.sqlstat import OracleSQLStatToday
from ..statistics.current_task.risk_rule import OracleStatsSchemaRiskRule
from ..tasks.capture import OracleCMDBTaskCapture
from utils.schema_utils import *
from rule.const import ALL_RULE_LEVELS,RULE_ENTRY_ONLINE_SQL
from models.sqlalchemy import make_session
from restful_api.modules import as_view


@as_view(group="online")
class RiskRuleHandler(OraclePrivilegeReq):

    def get(self):
        """库触犯的风险规则，
        entry="OBJECT"、"SQL"为触犯的obj规则或sql规则,
        获取某几天的触犯的规则某天为当天最后一次采集结果集
        """
        params = self.get_query_args(Schema({
            "cmdb_id": scm_int,

            "entry": scm_empty_as_optional(scm_one_of_choices(OracleStatsSchemaRiskRule.issue_entries())),
            "date_start": scm_date,
            "date_end": scm_date_end,

            scm_optional("schema_name", default=None): scm_str,
            scm_optional("rule_name", default=None): scm_dot_split_str,
            scm_optional("level", default=None): scm_empty_as_optional(
                scm_one_of_choices(ALL_RULE_LEVELS)),
            **self.gen_p()
        }))
        cmdb_id = params.pop("cmdb_id")
        entry = params.pop("entry")
        date_start = params.pop("date_start")
        date_end = params.pop("date_end")
        schema_name = params.pop("schema_name")
        rule_name: Union[list, None] = params.pop("rule_name")
        level = params.pop("level")
        p = self.pop_p(params)

        with make_session() as session:
            cmdb_task = session.query(OracleCMDBTaskCapture).filter(
                OracleCMDBTaskCapture.cmdb_id == cmdb_id).first()
            date_latest_task_record = cmdb_task.day_last_succeed_task_record_id(
                date_start=date_start,
                date_end=date_end
            )
            risk_rule_q = OracleStatsSchemaRiskRule.filter(
                task_record_id__in=list(date_latest_task_record.values()),
                cmdb_id=cmdb_id,
                entry=entry
            )

            if schema_name:
                risk_rule_q = risk_rule_q.filter(schema_name=schema_name)
            if rule_name:
                risk_rule_q = risk_rule_q.filter(rule__desc__in=rule_name)
            if level:
                risk_rule_q = risk_rule_q.filter(level=level)
            risk_rule = [x.to_dict() for x in risk_rule_q]
            risk_rule, p = self.paginate(risk_rule, **p)
            self.resp(risk_rule, **p)

    get.argument = {
        "querystring": {
            "cmdb_id": "2526",
            "entry": "SQL",
            "date_start": "2020-05-20",
            "date_end": "2020-05-21",
            "//schema_name": "APEX_040200",
            "//rule_name": "序列CACHESIZE过小",
            "//level": "2",
            "//page": "1",
            "//per_page": "10"
        }
    }


@as_view("sql", group="online")
class RiskRuleSQLHandler(OraclePrivilegeReq):

    def get(self):
        """触犯规则的sql,
        库一次采集一个schema触犯一个规则的sql
        (触犯一个规则的一个sql会出现多个相同sql因为一个sql_id有多个plan_hash_value),
        entry=SQL,
        平均时间和执行次数为today当前today采集,
        (注意:sql_id下有的plan_hash_value并没有执行特征所有会有为None的,
        平均时间为0证明:短时间内执行很多很多次结果保留3为小数目前)
        执行次数直接来自采集execution_total 字段。
        平均时间直接来自采集elapsed_time_total/execution_total。"""
        params = self.get_query_args(Schema({
            "cmdb_id": scm_int,
            "schema_name": scm_str,
            "rule_name": scm_str,
            "task_record_id": scm_int,
            "entry": scm_one_of_choices(RULE_ENTRY_ONLINE_SQL),
            **self.gen_p()
        }))
        entry = params.pop("entry")
        p = self.pop_p(params)

        sql_issue_q = OracleOnlineSQLIssue.objects(**params).filter(entries__in=list(entry))

        _ = params.pop("rule_name")
        today_sqlstat_q = OracleSQLStatToday.filter(**params)
        sql_issue = []
        for sql_issue_q_o in sql_issue_q:
            sql_issue_o = sql_issue_q_o.to_dict()

            # 考虑到触犯规则记录了每一个plan_hash_value维度，一个sql多个plan_hash_value，真正产生执行特征的是其中一个plan_hash_value或几个
            sql_issue_o['executions_total'] = None
            sql_issue_o['execution_time_cost_on_average'] = None

            for today_sqlstat_q_o in today_sqlstat_q:
                if sql_issue_o['output_params']['sql_id'] == today_sqlstat_q_o.sql_id \
                        and sql_issue_o['output_params']['plan_hash_value'] == today_sqlstat_q_o.plan_hash_value:
                    sql_issue_o['executions_total'] = today_sqlstat_q_o.executions_total
                    if sql_issue_o['executions_total']:
                        sql_issue_o['execution_time_cost_on_average'] = round(
                            round(today_sqlstat_q_o.elapsed_time_total, 2) / sql_issue_o['executions_total'], 3)
            sql_issue.append(sql_issue_o)

        sql_issue, p = self.paginate(sql_issue, **p)
        self.resp(sql_issue, **p)

    get.argument = {
        "querystring": {
            "cmdb_id": "2526",
            "schema_name": "ISQLAUDIT_DEV",
            "rule_name": "SQL_LOOP_NUM",
            "task_record_id": "39",
            "entry": "SQL"
        }
    }
