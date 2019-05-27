# Author: kk.Fang(fkfkbill@gmail.com)

from schema import Schema, Optional, Or

from utils.schema_utils import *
from .base import *
from utils import rule_utils, cmdb_utils
from models.mongo import *
from models.oracle import *


class RuleRepoHandler(AuthReq):
    def get(self):
        """规则库列表"""
        params = self.get_query_args(Schema({
            Optional("rule_type"): scm_one_of_choices(rule_utils.ALL_RULE_TYPE),
            Optional("keyword", default=None): scm_str,
            Optional("page", default=1): scm_int,
            Optional("per_page", default=10): scm_int,
        }))
        keyword = params.pop("keyword")
        p = self.pop_p(params)
        rules = Rule.objects(**params)
        if keyword:
            rules = self.query_keyword(rules, keyword,
                                       "db_model", "rule_desc", "rule_name", "rule_summary")
        items, p = self.paginate(rules, **p)
        self.resp([i.to_dict() for i in items], **p)

    def post(self):
        """新增规则"""
        params = self.get_json_args(Schema({
            "db_type": scm_one_of_choices(cmdb_utils.ALL_SUPPORTED_DB_TYPE),
            "db_model": scm_one_of_choices(rule_utils.ALL_SUPPORTED_MODEL),
            "exclude_obj_type": [scm_unempty_str],
            "input_parms": [
                {
                    "parm_desc": scm_str,
                    "parm_name": scm_str,
                    "parm_unit": scm_str,
                    "parm_value": Or(float, int, str)
                }
            ],
            "max_score": scm_int,
            "output_parms": [
                {
                    "parm_desc": scm_str,
                    "parm_name": scm_str
                }
            ],
            "rule_desc": scm_str,
            "rule_name": scm_unempty_str,
            "rule_complexity": scm_str,
            "rule_cmd": scm_str,
            "rule_status": scm_one_of_choices(rule_utils.ALL_RULE_STATUS),
            "rule_summary": scm_str,
            "rule_type": scm_str,
            "solution": [scm_unempty_str],
            "weight": scm_float
        }))
        new_rule = Rule(**params)
        new_rule.save()
        self.resp_created(new_rule.to_dict())

    def patch(self):
        """修改规则"""
        params = self.get_json_args(Schema({
            "rule_name": self.scm_with_em(scm_unempty_str, e="规则名称不能为空"),

            Optional("db_type"): scm_one_of_choices(cmdb_utils.ALL_SUPPORTED_DB_TYPE),
            Optional("db_model"): scm_one_of_choices(rule_utils.ALL_SUPPORTED_MODEL),
            Optional("exclude_obj_type"): [scm_unempty_str],
            Optional("input_parms"): [
                {
                    "parm_desc": scm_str,
                    "parm_name": scm_str,
                    "parm_unit": scm_str,
                    "parm_value": Or(float, int, str)
                }
            ],
            Optional("max_score"): scm_int,
            Optional("output_parms"): [
                {
                    "parm_desc": scm_str,
                    "parm_name": scm_str
                }
            ],
            Optional("rule_desc"): scm_str,
            Optional("rule_complexity"): scm_str,
            Optional("rule_cmd"): scm_str,
            Optional("rule_status"): scm_one_of_choices(rule_utils.ALL_RULE_STATUS),
            Optional("rule_summary"): scm_str,
            Optional("rule_type"): scm_str,
            Optional("solution"): [scm_unempty_str],
            Optional("weight"): scm_float
        }))
        rule_name = params.pop("rule_name")
        rule = Rule.objects(rule_name=rule_name).first()
        rule.from_dict(params)
        self.resp_created(rule.to_dict())

    # def delete(self):
    #     """删除规则"""
    #     self.resp_created()


class RiskRuleHandler(AuthReq):

    def get(self):
        """风险规则列表"""
        params = self.get_query_args(Schema({
            Optional("rule_type"): scm_one_of_choices(rule_utils.ALL_RULE_TYPE),
            Optional("keyword", default=None): scm_str,
            Optional("page", default=1): scm_int,
            Optional("per_page", default=10): scm_int,
        }))
        keyword = params.pop("keyword")
        p = self.pop_p(params)
        with make_session() as session:
            q = session.query(RiskSQLRule).filter_by(**params)
            if keyword:
                mq = Rule.objects()  # search in mongo
                rule_name_list_in_m = self.query_keyword(mq, keyword,
                                                  "db_model",
                                                  "rule_desc",
                                                  "rule_name",
                                                  "rule_summary").values_list("rule_name", flat=True)
                # search in oracle
                rule_name_list_in_o = [i[0] for i in self.query_keyword(q, keyword,
                                                       RiskSQLRule.risk_name,
                                                       RiskSQLRule.severity,
                                                       RiskSQLRule.rule_name,
                                                       RiskSQLRule.optimized_advice).\
                    with_entities(RiskSQLRule.rule_name).all()]
                q = q.filter(RiskSQLRule.rule_name.in_(
                    rule_name_list_in_m + rule_name_list_in_o))

            risk_rules, p = self.paginate(q, **p)
            ret = []
            for risk_rule in risk_rules:
                ret.append(rule_utils.merge_risk_rule_and_rule(risk_rule))
            self.resp(ret, **p)

    def post(self):
        """增加风险规则"""
        params = self.get_json_args(Schema({
            "risk_name": scm_unempty_str,
            "severity": scm_str,
            "optimized_advice": scm_str,
            Optional("db_type", default=cmdb_utils.DB_ORACLE): scm_one_of_choices(cmdb_utils.ALL_SUPPORTED_DB_TYPE),
            "db_model": scm_one_of_choices(rule_utils.ALL_SUPPORTED_MODEL),
            "rule_name": scm_unempty_str,
            "rule_type": scm_one_of_choices(rule_utils.ALL_RULE_TYPE),
        }))
        with make_session() as session:
            risk_rule = RiskSQLRule(**params)
            session.add(risk_rule)
            session.commit()
            session.refresh(risk_rule)
            self.resp_created(rule_utils.merge_risk_rule_and_rule(risk_rule))

    def patch(self):
        """修改风险规则"""
        params = self.get_json_args(Schema({
            "risk_sql_rule_id": scm_int,

            Optional("risk_name"): scm_unempty_str,
            Optional("severity"): scm_str,
            Optional("optimized_advice"): scm_str,
        }))
        risk_sql_rule_id = params.pop("risk_sql_rule_id")
        with make_session() as session:
            risk_rule = session.query(RiskSQLRule).\
                filter_by(risk_sql_rule_id=risk_sql_rule_id).first()
            if not risk_rule:
                self.resp_not_found(f"未找到风险规则id={risk_sql_rule_id}")
                return
            risk_rule.from_dict(params)
            session.add(risk_rule)
            session.commit()
            session.refresh(risk_rule)
            self.resp_created(rule_utils.merge_risk_rule_and_rule(risk_rule))

    def delete(self):
        """删除风险规则"""
        params = self.get_json_args(Schema({
            "risk_sql_rule_id": scm_int,
        }))
        with make_session() as session:
            session.query(RiskSQLRule).filter_by(**params).delete()
        self.resp_created(msg="已删除")
