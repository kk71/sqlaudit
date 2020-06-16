# Author: kk.Fang(fkfkbill@gmail.com)

from mongoengine import NotUniqueError

import cmdb.const
from restful_api.modules import *
from utils.schema_utils import *
from ..rule_cartridge import RuleCartridge
from .base import *


@as_view(group="rule-cartridge")
class RuleCartridgeHandler(BaseRuleHandler):

    def get(self):
        """墨盒规则列表"""
        params = self.get_query_args(Schema({
            scm_optional("db_type"): self.scm_one_of_choices(
                cmdb.const.ALL_DB_TYPE),
            scm_optional("db_model"): self.scm_one_of_choices(
                cmdb.const.ALL_DB_MODEL),
            scm_optional("status"): scm_empty_as_optional(scm_bool),
            scm_optional("keyword", default=None): scm_str,
            **self.gen_p()
        }))
        keyword = params.pop("keyword")
        p = self.pop_p(params)

        rc_q = RuleCartridge.filter(**params)
        if keyword:
            rc_q = self.query_keyword(rc_q, keyword,
                                      "name", "desc", "db_type", "summary")
        ret, p = self.paginate(rc_q, **p)
        self.resp([i.to_dict() for i in ret], **p)

    get.argument = {
        "querystring": {
            "//db_type": "oracle",
            "//db_model": "OLTP",
            "//keyword": "em",
            "//status": "0",
            "//page": 1,
            "//per_page": 10
        }
    }

    def post(self):
        """新增墨盒规则"""
        params = self.get_json_args(Schema({
            "db_model": self.scm_one_of_choices(cmdb.const.ALL_DB_MODEL),
            **self.base_rule_schema_for_adding()
        }))
        new_rc = RuleCartridge(**params)
        self.test_code(new_rc)
        try:
            new_rc.save()
        except NotUniqueError:
            return self.resp_bad_req(msg=f"{new_rc.unique_key()} 已存在")
        self.resp_created(new_rc.to_dict())

    post.argument = {
        "json": {
            "db_type": "oracle",
            "db_model": "OLTP",
            "desc": "",
            "entries": ["ONLINE"],
            "input_params": [{
                "name": "字段名",
                "desc": "描述文本",
                "unit": "",
                "data_type": "STR",
                "value": "emm"
            }],
            "output_params": [{
                "name": "字段名",
                "desc": "描述文本",
                "unit": "",
                "data_type": "STR",
                "optional": False
            }],
            "code": "def code(rule, entries, **kwargs): \n\n    pass\ncode_hole.append(code)",
            "status": True,
            "summary": "",
            "solution": [],
            "weight": 5.0,
            "max_score": 20,
            "level": 1
        }
    }

    def put(self):
        """通用修改墨盒规则"""
        params = self.get_json_args(Schema({
            "db_type": self.scm_one_of_choices(cmdb.const.ALL_DB_TYPE),
            "db_model": self.scm_one_of_choices(cmdb.const.ALL_DB_MODEL),
            "name": scm_unempty_str,

            **self.base_rule_schema_for_whole_updating()
        }))
        db_type = params.pop("db_type")
        name = params.pop("name")
        db_model = params.pop("db_model")

        the_rc = RuleCartridge.filter(
            db_type=db_type,
            name=name,
            db_model=db_model).first()
        the_rc.from_dict(params)
        self.test_code(the_rc)
        the_rc.save()
        self.resp_created(the_rc.to_dict())

    put.argument = {
        "json": {
            "db_type": "oracle",
            "db_model": "OLTP",
            "name": "emm"
        }
    }

    def patch(self):
        """修改墨盒规则(输入参数，输出参数，entries)"""
        pass
        params = self.get_json_args(Schema({
            "db_type": self.scm_one_of_choices(cmdb.const.ALL_DB_TYPE),
            "db_model": self.scm_one_of_choices(cmdb.const.ALL_DB_MODEL),
            "name": scm_unempty_str,

            **self.base_rule_schema_for_special_updating()
        }))
        db_type = params.pop("db_type")
        name = params.pop("name")
        db_model = params.pop("db_model")

        the_cr = RuleCartridge.filter(
            db_type=db_type,
            name=name,
            db_model=db_model
        ).first()
        self.special_update(the_cr, **params)
        self.save()
        self.resp_created(the_cr.to_dict())

    def delete(self):
        """删除墨盒规则"""
        params = self.get_json_args(Schema({
            "db_type": self.scm_one_of_choices(cmdb.const.ALL_DB_TYPE),
            "db_model": self.scm_one_of_choices(cmdb.const.ALL_DB_MODEL),
            "name": scm_unempty_str
        }))
        the_rc = RuleCartridge.filter(**params).first()
        if not the_rc:
            return self.resp_bad_req(msg=f"{params}规则未找到")
        the_rc.delete()
        self.resp_created(msg="已删除")

    delete.argument = {
        "json": {
            "db_type": "oracle",
            "db_model": "OLTP",
            "name": "emm"
        }
    }
