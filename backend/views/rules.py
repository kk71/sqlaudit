# Author: kk.Fang(fkfkbill@gmail.com)

import arrow
from schema import Schema, Optional, And, Or
from sqlalchemy.exc import IntegrityError

import settings
from backend.utils.schema_utils import *
from backend.views.base import *
from backend.models.oracle import *
from backend.models.mongo import *
from backend.utils import rule_utils


class RuleRepoHandler(AuthReq):
    def get(self):
        """规则库列表"""
        params = self.get_query_args(Schema({
            Optional("page", default=1): scm_int,
            Optional("per_page", default=10): scm_int,
        }))
        rules = Rule.objects().all()
        items, p = self.paginate(rules, **params)
        self.resp([i.to_dict() for i in items], **p)

    def post(self):
        """新增规则"""
        params = self.get_json_args(Schema({
            "db_type": scm_one_of_choices(rule_utils.ALL_SUPPORTED_DB),
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

            Optional("db_type"): scm_one_of_choices(rule_utils.ALL_SUPPORTED_DB),
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