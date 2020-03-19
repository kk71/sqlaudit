# Author: kk.Fang(fkfkbill@gmail.com)

from schema import Optional, Schema

from new_rule.rule import *
from utils.schema_utils import *
from restful_api.views.base import PrivilegeReq
from utils.const import *


class TicketRuleHandler(PrivilegeReq):

    def get(self):
        """线下工单规则列表"""
        params = self.get_query_args(Schema({
            Optional("db_type"): scm_one_of_choices(ALL_SUPPORTED_DB_TYPE),
            Optional("keyword", default=None): scm_str,
            **self.gen_p()
        }))
        keyword = params.pop("keyword")
        p = self.pop_p(params)
        rules = TicketRule.objects(**params)
        if keyword:
            rules = self.query_keyword(rules, keyword, "desc", "name")
        items, p = self.paginate(rules, **p)
        self.resp([i.to_dict() for i in items], **p)

    def post(self):
        """新增规则"""
        self.acquire(PRIVILEGE.PRIVILEGE_RULE)

        params = self.get_json_args(Schema({
            "db_type": scm_one_of_choices(ALL_SUPPORTED_DB_TYPE),
            "input_params": [
                {
                    "desc": scm_str,
                    "name": scm_unempty_str,
                    "unit": scm_str,
                    "value": object,
                }
            ],
            "max_score": scm_num,
            "output_params": [
                {
                    "desc": scm_str,
                    "name": scm_unempty_str,
                    "unit": scm_str
                }
            ],
            "desc": scm_str,
            "name": scm_unempty_str,
            "code": scm_str,
            "status": scm_bool,
            "summary": scm_str,
            "sql_type": scm_one_of_choices(ALL_SQL_TYPE),
            "solution": [scm_unempty_str],
            "weight": scm_num,
            "analyse_type": scm_one_of_choices(ALL_TICKET_ANALYSE_TYPE)
        }))
        new_rule = TicketRule(**params)
        new_rule.run(test_only=True)
        new_rule.save()
        self.resp_created(new_rule.to_dict())

    def patch(self):
        """修改规则"""
        self.acquire(PRIVILEGE.PRIVILEGE_RULE)

        params = self.get_json_args(Schema({

            # 这二个字段是不能改的
            "db_type": scm_one_of_choices(ALL_SUPPORTED_DB_TYPE),
            "name": scm_unempty_str,

            Optional("input_params"): [
                {
                    "desc": scm_str,
                    "name": scm_unempty_str,
                    "unit": scm_str,
                    "value": object,
                }
            ],
            Optional("max_score"): scm_num,
            Optional("output_params"): [
                {
                    "desc": scm_str,
                    "name": scm_unempty_str,
                    "unit": scm_str
                }
            ],
            Optional("desc"): scm_str,
            Optional("code"): scm_str,
            Optional("status"): scm_bool,
            Optional("summary"): scm_str,
            Optional("type"): scm_str,
            Optional("sql_type"): scm_one_of_choices(ALL_SQL_TYPE),
            Optional("solution"): [scm_unempty_str],
            Optional("weight"): scm_num
        }))

        rule = TicketRule.objects(
            name=params.pop("name"),
            db_type=params.pop("db_type")).first()
        rule.from_dict(params)
        rule.run(test_only=True)
        rule.save()
        self.resp_created(rule.to_dict())


class TicketRuleCodeHandler(PrivilegeReq):

    def get(self):
        """获取空的规则code的模板"""
        self.resp({
            "code": TicketRule.code_template()
        })
