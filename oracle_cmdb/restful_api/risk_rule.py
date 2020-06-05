from typing import Union

from .base import OraclePrivilegeReq
from ..statistics.current_task.risk_sql import OracleStatsSchemaRiskSQL
from ..statistics.current_task.risk_rule import OracleStatsSchemaRiskRule
from ..statistics.current_task.risk_object import OracleStatsSchemaRiskObject
from ..tasks.capture import OracleCMDBTaskCapture
from utils.schema_utils import *
from rule.const import ALL_RULE_LEVELS
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
            scm_optional("level", default=None): self.scm_one_of_choices(
                ALL_RULE_LEVELS, use=scm_int),
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
                risk_rule_q = risk_rule_q.filter(rule__name__in=rule_name)
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
            "//rule_name": "SUBQUERY_SELECT",
            "//level": "2",
            "//page": "1",
            "//per_page": "10"
        }
    }


@as_view("sql", group="online")
class RiskRuleSQLHandler(OraclePrivilegeReq):

    def get(self):
        """触犯风险sql规则的sql,
        库一次采集一个schema触犯一个规则的sql,
        (违反数量为sql_id去重结果(出现多同一sql_id，因一个sql_id有多plan_hash_value))
        """
        params = self.get_query_args(Schema({
            "cmdb_id": scm_int,
            "schema_name": scm_str,
            "rule_name": scm_str,
            "task_record_id": scm_int,
            **self.gen_p()
        }))
        p = self.pop_p(params)

        risk_sql_q = OracleStatsSchemaRiskSQL.filter(**params)
        risk_sql_q, p = self.paginate(risk_sql_q, **p)
        self.resp([risk_sql.to_dict() for risk_sql in risk_sql_q], **p)

    get.argument = {
        "querystring": {
            "cmdb_id": "2526",
            "schema_name": "ISQLAUDIT_DEV",
            "rule_name": "CHECK_LOB_USING",
            "task_record_id": "54",
        }
    }


@as_view("obj", group="online")
class RiskRuleOBJHandler(OraclePrivilegeReq):

    def get(self):
        """触犯风险obj规则的obj"""
        params = self.get_query_args(Schema({
            "cmdb_id": scm_int,
            "schema_name": scm_str,
            "rule_name": scm_str,
            "task_record_id": scm_int,
            **self.gen_p()
        }))
        p = self.pop_p(params)
        risk_obj_q = OracleStatsSchemaRiskObject.filter(**params)
        risk_obj_data = []
        for risk_obj in risk_obj_q:
            risk_obj = risk_obj.to_dict()
            risk_obj['issue_description_str'] = ""
            for x in risk_obj['issue_description']:
                risk_obj['issue_description_str'] += f"{str(x['desc'])}:{str(x['value'])},"
            risk_obj_data.append(risk_obj)
        risk_obj_data, p = self.paginate(risk_obj_data, **p)
        self.resp(risk_obj_data, **p)

    get.argument = {
        "querystring": {
            "cmdb_id": "2526",
            "schema_name": "DVSYS",
            "rule_name": "SEQ_CACHESIZE",
            "task_record_id": "56"
        }
    }
