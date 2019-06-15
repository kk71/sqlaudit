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
            Optional("cmdb_id", default=None): scm_int,
            Optional("rule_id", default=None): scm_int
        }))
        cmdb_id, rule_id = params.pop("cmdb_id"), params.pop("rule_id")
        del params

        with make_session() as session:
            if cmdb_id:
                whitelist = session.query(WhiteListRules).filter_by(cmdb_id=cmdb_id)
                whitelist = [wl.to_dict() for wl in whitelist]
                for wl in whitelist:
                    wl['rule_catagory'] = {1: '过滤用户', 2: '过滤程序modal', 3: '过滤sqltext', 4: '过滤规则'}[wl['rule_catagory']]
                    wl['status'] = '启用' if wl['status'] == 0 else '禁用'

                self.resp({'whitelist_rules': whitelist, 'cmdb_id': cmdb_id})
            elif rule_id:
                rule = session.query(WhiteListRules).filter_by(id=rule_id)
                rule = [r.to_dict() for r in rule]

                self.resp({'data': rule})
            else:
                self.resp_bad_req("参数不正确")

    def patch(self):
        """禁用启用,及编辑"""
        params = self.get_json_args(Schema({
            "rule_id": scm_int,

            Optional("comments", default=None): scm_str,
            Optional("rule_type", default=None): scm_int,
            Optional("rule_name", default=None): scm_unempty_str,
            Optional("rule_text", default=None): scm_unempty_str,

            # 禁用启用
            Optional("status", default=None): scm_int,
        }))
        comments, rule_type, rule_name, rule_text = params.pop('comments'), params.pop('rule_type'), \
                                                    params.pop('rule_name'), params.pop('rule_text')
        status = params.pop('status')
        rule_id = params.pop('rule_id')
        del params

        with make_session() as session:
            a = session.query(WhiteListRules).filter(WhiteListRules.id == rule_id). \
                update({'status': status, 'comments': comments,
                        'rule_catagory': rule_type, 'rule_name': rule_name,
                        'rule_text': rule_text})

            self.resp_created(msg="修改白名单规则成功")

    def post(self):
        """新增"""
        params = self.get_json_args(Schema({
            'cmdb_id': scm_int,

            'rule_name': scm_unempty_str,
            'rule_text': scm_unempty_str,
            'rule_type': scm_int,
            'status': scm_int,
            Optional('comments', default=None): scm_str
        }))
        rule_name, rule_text = params.pop('rule_name'), params.pop('rule_text')
        rule_type, status = params.pop('rule_type'), params.pop('status')
        cmdb_id, comments = params.pop('cmdb_id'), params.pop('comments')
        del params
        login_user = self.current_user

        with make_session() as session:
            W = WhiteListRules()
            W.cmdb_id = cmdb_id
            W.rule_name = rule_name
            W.rule_catagory = rule_type
            W.rule_text = rule_text
            W.status = status
            W.comments = comments
            W.create_date = datetime.now()
            W.creator = login_user
            session.add(W)

            cmdb_rule_counts = session.query(CMDB).filter_by(cmdb_id=cmdb_id). \
                update({CMDB.while_list_rule_counts: CMDB.while_list_rule_counts + 1})

            self.resp_created('添加白名单成功')

    def delete(self):
        params = self.get_query_args(Schema({
            "rule_id": scm_int
        }))
        rule_id = params.pop("rule_id")
        del params

        with make_session() as session:
            cmdb = session.query(WhiteListRules).filter_by(id=rule_id).with_entities(WhiteListRules.cmdb_id)
            cmdb = [list(x)[0] for x in cmdb]
            if len(cmdb) == 0:
                return self.resp_bad_req("无效的cmdb")

            session.query(WhiteListRules).filter_by(id=rule_id).delete()
            session.query(CMDB).filter_by(cmdb_id=cmdb[0]).update(
                {"while_list_rule_counts": CMDB.while_list_rule_counts - 1})

            self.resp_created("删除规则成功")
