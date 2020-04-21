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

            scm_optional("keyword", default=None): scm_str,
            **self.gen_p()
        }))
        keyword = params.pop("keyword")
        p = self.pop_p(params)

        rc_q = RuleCartridge.objects(**params)
        if keyword:
            rc_q = self.query_keyword(rc_q, keyword,
                                      "name", "desc", "db_type", "summary")
        ret, p = self.paginate(rc_q, **p)
        self.resp([i.to_dict() for i in ret], **p)

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

        the_rc = RuleCartridge.objects(
            db_type=db_type,
            name=name,
            db_model=db_model).first()
        the_rc.from_dict(params)
        self.test_code(the_rc)
        the_rc.save()
        self.resp_created(the_rc.to_dict())

    def patch(self):
        """修改墨盒规则(输入参数，输出参数，入口)"""
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

        the_cr = RuleCartridge.objects(
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
        the_rc = RuleCartridge.objects(**params).first()
        if not the_rc:
            return self.resp_bad_req(msg=f"{params}规则未找到")
        the_rc.delete()
        self.resp_created(msg="已删除")
