from typing import Union

from .base import OraclePrivilegeReq
from ..const import ONLINE_RISK_RULE_ENTRIES
from ..statistics.current_task.risk_rule import OracleStatsSchemaRiskRule
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

            "entry": scm_empty_as_optional(scm_one_of_choices(ONLINE_RISK_RULE_ENTRIES)),
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
            cmdb_task = session.query(OracleCMDBTaskCapture).filter(OracleCMDBTaskCapture.cmdb_id == cmdb_id).first()
            date_latest_task_record = cmdb_task.day_last_succeed_task_record_id(
                date_start=date_start,
                date_end=date_end
            )
            date_latest_task_record = list(date_latest_task_record.values())

            risk_rule_q = OracleStatsSchemaRiskRule.filter(cmdb_id=cmdb_id, entry=entry,
                                                           task_record_id__in=date_latest_task_record)

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
            "page": "1",
            "per_page": "10"
        }
    }
