# Author: kk.Fang(fkfkbill@gmail.com)

from schema import Schema, Optional, Or

from utils.schema_utils import *
from utils.const import *
from .base import *
from utils import rule_utils
from models.mongo import *
from models.oracle import *


class RuleRepoHandler(AuthReq):
    def get(self):
        """规则库列表"""
        params = self.get_query_args(Schema({
            Optional("rule_type"): scm_one_of_choices(ALL_RULE_TYPE),
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
            "db_type": scm_one_of_choices(ALL_SUPPORTED_DB_TYPE),
            "db_model": scm_one_of_choices(ALL_SUPPORTED_MODEL),
            "exclude_obj_type": scm_dot_split_str,
            "input_parms": [
                {
                    "parm_desc": scm_str,
                    "parm_name": scm_unempty_str,
                    "parm_unit": scm_str,
                    "parm_value": Or(float, int, str),

                    Optional(object): object
                }
            ],
            "max_score": scm_int,
            "output_parms": [
                {
                    "parm_desc": scm_str,
                    "parm_name": scm_unempty_str,

                    Optional(object): object
                }
            ],
            "rule_desc": scm_str,
            "rule_name": scm_unempty_str,
            "rule_complexity": scm_str,
            "rule_cmd": scm_str,
            "rule_status": scm_one_of_choices(ALL_RULE_STATUS),
            "rule_summary": scm_str,
            "rule_type": scm_str,
            "solution": [scm_unempty_str],
            "weight": scm_float
        }))
        params["input_parms"] = [{k: v for k, v in i.items() if k in (
            "parm_desc", "parm_name", "parm_unit", "parm_value"
        )} for i in params["input_parms"]]
        params["output_parms"] = [{k: v for k, v in i.items() if k in (
            "parm_desc", "parm_name"
        )} for i in params["output_parms"]]
        new_rule = Rule(**params)
        new_rule.save()
        self.resp_created(new_rule.to_dict())

    def patch(self):
        """修改规则"""
        params = self.get_json_args(Schema({
            "_id": scm_unempty_str,

            # Optional("db_type"): scm_one_of_choices(ALL_SUPPORTED_DB_TYPE),
            # Optional("db_model"): scm_one_of_choices(ALL_SUPPORTED_MODEL),
            # Optional("rule_name"): scm_unempty_str,
            # Optional("input_parms"): [
            #     {
            #         "parm_desc": scm_str,
            #         "parm_name": scm_str,
            #         "parm_unit": scm_str,
            #         "parm_value": Or(float, int, str)
            #     }
            # ],
            Optional("max_score"): scm_int,
            # Optional("output_parms"): [
            #     {
            #         "parm_desc": scm_str,
            #         "parm_name": scm_str
            #     }
            # ],
            # Optional("rule_desc"): scm_str,
            # Optional("rule_complexity"): scm_str,
            # Optional("rule_cmd"): scm_str,
            Optional("rule_status"): scm_one_of_choices(ALL_RULE_STATUS),
            # Optional("rule_summary"): scm_str,
            # Optional("rule_type"): scm_str,
            # Optional("solution"): [scm_unempty_str],
            Optional("weight"): scm_float
        }))
        rule_id = params.pop("_id")
        rule = Rule.objects(_id=rule_id).first()
        rule.from_dict(params)
        self.resp_created(rule.to_dict())


class RiskRuleHandler(AuthReq):

    def get(self):
        """风险规则列表"""
        params = self.get_query_args(Schema({
            Optional("rule_type"): scm_one_of_choices(ALL_RULE_TYPE),
            Optional("keyword", default=None): scm_str,
            Optional("page", default=1): scm_int,
            Optional("per_page", default=10): scm_int,
        }))
        keyword = params.pop("keyword")
        p = self.pop_p(params)
        with make_session() as session:
            q = session.query(RiskSQLRule).filter_by(**params)
            if keyword:
                mq = Rule.filter_enabled()  # search in mongo
                rule_name_list_in_m = list(self.query_keyword(mq, keyword,
                                                              "db_model",
                                                              "rule_desc",
                                                              "rule_name",
                                                              "rule_summary").values_list("rule_name"))
                # search in oracle
                rule_name_list_in_o = [i[0] for i in self.query_keyword(q, keyword,
                                                                        RiskSQLRule.risk_name,
                                                                        RiskSQLRule.severity,
                                                                        RiskSQLRule.rule_name,
                                                                        RiskSQLRule.optimized_advice). \
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
            Optional("db_type", default=DB_ORACLE): scm_one_of_choices(ALL_SUPPORTED_DB_TYPE),
            "db_model": scm_one_of_choices(ALL_SUPPORTED_MODEL),
            "rule_name": scm_unempty_str,
            "rule_type": scm_one_of_choices(ALL_RULE_TYPE),
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
            risk_rule = session.query(RiskSQLRule). \
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


class WhiteListHandler(AuthReq):

    def get(self):
        """风险白名单详情页,及编辑时查询"""
        params = self.get_query_args(Schema({
            "cmdb_id": scm_int,

            Optional("keyword", default=None): scm_str,
            Optional("page", default=1): scm_int,
            Optional("per_page", default=10): scm_int,
        }))
        p = self.pop_p(params)
        keyword = params.pop("keyword")

        with make_session() as session:
            whitelist_q = session.query(WhiteListRules).filter_by(**params)
            if keyword:
                whitelist_q = self.query_keyword(whitelist_q, keyword,
                                                 WhiteListRules.rule_name,
                                                 WhiteListRules.rule_text,
                                                 WhiteListRules.comments)
            items, p = self.paginate(whitelist_q, **p)
            self.resp(items, **p)

    def patch(self):
        """禁用启用,及编辑"""
        params = self.get_json_args(Schema({
            "id": scm_int,

            Optional("rule_name", default=None): scm_unempty_str,
            Optional("rule_category", default=None): scm_one_of_choices(
                const.ALL_WHITE_LIST_CATEGORY),
            Optional("rule_text", default=None): scm_unempty_str,
            Optional("status", default=None): scm_bool,
            Optional("comments", default=None): scm_str,
        }))
        rule_id = params.pop('id')

        with make_session() as session:
            session.query(WhiteListRules).filter(WhiteListRules.id == rule_id).update(params)
        self.resp_created(msg="修改白名单规则成功")

    def post(self):
        """新增"""
        params = self.get_json_args(Schema({
            'cmdb_id': scm_int,

            'rule_name': scm_unempty_str,
            'rule_text': scm_unempty_str,
            'rule_category': scm_one_of_choices(const.ALL_WHITE_LIST_CATEGORY),
            'status': scm_bool,
            Optional('comments', default=None): scm_str
        }))

        with make_session() as session:
            w = WhiteListRules()
            w.from_dict(params)
            session.add(w)
            session.commit()
            session.query(CMDB).filter_by(cmdb_id=params["cmdb_id"]). \
                update({CMDB.while_list_rule_counts: CMDB.while_list_rule_counts + 1})
            session.refresh(w)
            self.resp_created(w.to_dict())

    def delete(self):
        params = self.get_query_args(Schema({
            "id": scm_int
        }))
        with make_session() as session:
            w = session.query(WhiteListRules).filter_by(**params).first()
            session.query(CMDB).filter_by(cmdb_id=w.cmdb_id).update(
                {"while_list_rule_counts": CMDB.while_list_rule_counts - 1})
            session.delete(w)
        self.resp_created("删除成功")
