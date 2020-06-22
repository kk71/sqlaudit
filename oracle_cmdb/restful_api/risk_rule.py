from os import path
from typing import Union

import settings
from .base import OraclePrivilegeReq
from ..capture.sqltext import OracleSQLText
from ..statistics.current_task.risk_sql import OracleStatsSchemaRiskSQL
from ..statistics.current_task.risk_rule import OracleStatsSchemaRiskRule
from ..statistics.current_task.risk_object import OracleStatsSchemaRiskObject
from ..tasks.capture import OracleCMDBTaskCapture
from ..tasks.risk_obj_export import RiskRuleObjExport
from ..tasks.risk_sql_export import RiskRuleSqlExport
from utils.schema_utils import *
from utils.datetime_utils import *
from rule.const import ALL_RULE_LEVELS
from models.sqlalchemy import make_session
from restful_api.modules import as_view

class GetRiskRuleBase:

    def get_risk_rule(self, session, **params):
        cmdb_id = params.pop("cmdb_id")
        entry = params.pop("entry")
        date_start = params.pop("date_start")
        date_end = params.pop("date_end")
        schema_name = params.get("schema_name")
        rule_name: Union[list, None] = params.get("rule_name")
        level = params.get("level")

        cmdb_task = session.query(OracleCMDBTaskCapture).filter(
            OracleCMDBTaskCapture.cmdb_id == cmdb_id).first()
        date_latest_task_record = cmdb_task.day_last_succeed_task_record_id(
            date_start=date_start,
            date_end=date_end
        )
        task_record_id_list = list(date_latest_task_record.values())
        risk_rule_q = OracleStatsSchemaRiskRule.filter(
            task_record_id__in=task_record_id_list,
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

        return risk_rule, cmdb_id, task_record_id_list


@as_view(group="online")
class RiskRuleHandler(OraclePrivilegeReq,GetRiskRuleBase):


    def filter_params(self):

        params = self.get_query_args(Schema({
            "cmdb_id": scm_int,

            "entry": scm_empty_as_optional(scm_one_of_choices(OracleStatsSchemaRiskRule.issue_entries())),
            "date_start": scm_date,
            "date_end": scm_date_end,

            scm_optional("schema_name", default=None): scm_str,
            scm_optional("rule_name", default=None): scm_dot_split_str,
            scm_optional("level", default=None): self.scm_one_of_choices(
                ALL_RULE_LEVELS, use=scm_int),
            scm_optional(object): object
        }))
        return params

    def get(self):
        """库触犯的风险规则，
        entry="OBJECT"、"SQL"为触犯的obj规则或sql规则,
        获取某几天的触犯的规则某天为当天最后一次采集结果集
        """
        params = self.filter_params()
        page = self.get_query_args(Schema({
            **self.gen_p(),
            scm_optional(object): object
        }))
        p = self.pop_p(page)

        with make_session() as session:
            risk_rule, _, _ = self.get_risk_rule(session, **params)

            risk_rule, p = self.paginate(risk_rule, **p)
            self.resp(risk_rule, **p)

    get.argument = {
        "querystring": {
            "cmdb_id": "13",
            "entry": "SQL",
            "date_start": "2020-06-10",
            "date_end": "2020-06-10",
            "//schema_name": "ISQLAUDIT_DEV",
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

class RiskRuleObj:

    def get_risk_obj(self, **params):

        risk_obj_q = OracleStatsSchemaRiskObject.filter(**params)
        risk_obj_data = []
        for risk_obj in risk_obj_q:
            risk_obj = risk_obj.to_dict()
            risk_obj['issue_description_str'] = ""
            for x in risk_obj['issue_description']:
                risk_obj['issue_description_str'] += f"{str(x['desc'])}:{str(x['value'])},"
            risk_obj_data.append(risk_obj)
        return risk_obj_data


@as_view("obj", group="online")
class RiskRuleOBJHandler(OraclePrivilegeReq,RiskRuleObj):

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
        risk_obj_data = self.get_risk_obj(**params)
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

class RiskRuleSql:

    def risk_rule_sql_inner(self,risk_rule_outer,cmdb_id,task_record_id_list):
        risk_rule_sql_inner_q = OracleStatsSchemaRiskSQL.filter(cmdb_id=cmdb_id,
                                                                task_record_id__in=task_record_id_list)
        for x in risk_rule_sql_inner_q:  # TODO应该统计的时候写入sql_text
            sql_text = OracleSQLText.filter(sql_id=x.sql_id).first()
            x.sql_text = sql_text['longer_sql_text']

        parame_dict = {
            "risk_rule_outer": risk_rule_outer,
            "risk_rule_sql_inner": risk_rule_sql_inner_q
        }
        return parame_dict


@as_view("sql_export", group="online")
class RiskSqlExportHandler(RiskRuleHandler,RiskRuleSql):

    async def post(self):
        """风险SQL导出
        导出分为四种:
        1.导出所有cmdb,时间。
        2.导出所有cmdb,时间,(schema,rule_name,等级)
        3.导出已选cmdb,时间,rule_name
        4.导出已选cmdb,时间,rule_name,(schema,等级)"""
        params = self.filter_params()

        filename = f"risk_rule_sql_{dt_to_str(arrow.now())}.xlsx"
        await RiskRuleSqlExport.async_shoot(filename=filename, parame_dict=params)
        await self.resp({"url": path.join(settings.EXPORT_PREFIX, filename)})

    post.argument = {
        "querystring": {
            "cmdb_id": "13",
            "entry": "SQL",
            "date_start": "2020-06-10",
            "date_end": "2020-06-10",

            "//schema_name": "SQLAUDIT_TEST",
            "//rule_name": "IDX_PARALLEL_ON",
            "//level": "2"
        }
    }


@as_view("obj_export", group="online")
class RiskObjectExportHandler(RiskRuleHandler, RiskRuleOBJHandler):

    async def post(self):
        """风险对象导出
         导出分为四种:
        1.导出所有cmdb,时间。
        2.导出所有cmdb,时间,(schema,rule_name,等级)
        3.导出已选cmdb,时间,rule_name
        4.导出已选cmdb,时间,rule_name,(schema,等级)
        """
        params = self.filter_params()
        with make_session() as session:
            risk_rule_outer, cmdb_id, task_record_id_list = self.get_risk_rule(session, **params)
            risk_rule_obj_inner = self.get_risk_obj(cmdb_id=cmdb_id, task_record_id__in=task_record_id_list)

            parame_dict = {
                "risk_rule_outer": risk_rule_outer,
                "risk_rule_obj_inner": risk_rule_obj_inner
            }

            filename = f"risk_rule_obj_{dt_to_str(arrow.now())}.xlsx"
            await RiskRuleObjExport.async_shoot(filename=filename, parame_dict=params)
            await self.resp({"url": path.join(settings.EXPORT_PREFIX, filename)})

    post.argument = {
        "querystring": {
            "cmdb_id": "13",
            "entry": "OBJECT",
            "date_start": "2020-06-10",
            "date_end": "2020-06-10",

            "//schema_name": "SQLAUDIT_TEST",
            "//rule_name": "IDX_PARALLEL_ON",
            "//level": "2"
        }
    }
