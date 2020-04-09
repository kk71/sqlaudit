# Author: kk.Fang(fkfkbill@gmail.com)

import cmdb.const
from restful_api.modules import *
from utils.schema_utils import *
from ..rule import RuleCartridge
from .base import *


@as_view(group="rule-cartridge")
class RuleCartridgeHandler(BaseRuleHandler):

    def get(self):
        """墨盒规则列表"""
        params = self.get_query_args(Schema({
            scm_optional("db_type"): self.scm_one_of_choices(
                cmdb.const.ALL_DB_TYPE),
            scm_optional("status"): scm_bool,

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
            **self.base_rule_schema_for_adding()
        }))
        new_rc = RuleCartridge(**params)
        self.test_code(new_rc)
        new_rc.save()
        self.resp_created(new_rc.to_dict())

    def put(self):
        """通用修改墨盒规则"""
        params = self.get_json_args(Schema({
            "db_type": self.scm_one_of_choices(cmdb.const.ALL_DB_TYPE),
            "name": scm_unempty_str,
            **self.base_rule_schema_for_whole_updating()
        }))
        db_type = params.pop("db_type")
        name = params.pop("name")
        the_rc = RuleCartridge.objects(db_type=db_type, name=name).first()
        the_rc.from_dict(params)
        self.test_code(the_rc)
        the_rc.save()
        self.resp_created(the_rc.to_dict())

    def patch(self):
        """修改墨盒规则(输入参数，输出参数，入口)"""
        pass

    def delete(self):
        """删除墨盒规则"""
        params = self.get_json_args(Schema({
            "db_type": self.scm_one_of_choices(cmdb.const.ALL_DB_TYPE),
            "name": scm_unempty_str
        }))
        the_rc = RuleCartridge.objects(**params).first()
        the_rc.delete()
        self.resp_created(msg="已删除")
