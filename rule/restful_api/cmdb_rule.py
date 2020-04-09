# Author: kk.Fang(fkfkbill@gmail.com)

from restful_api.modules import *
from utils.schema_utils import *
from ..rule import CMDBRule
from .base import *


@as_view(group="cmdb-rule")
class CMDBRuleHandler(BaseRuleHandler):

    def get(self):
        """库规则列表"""
        params = self.get_query_args(Schema({
            "cmdb_id": scm_gt0_int,
            scm_optional("status"): scm_bool,

            scm_optional("keyword", default=None): scm_str,
            **self.gen_p()
        }))
        keyword = params.pop("keyword")
        p = self.pop_p(params)

        cr_q = CMDBRule.objects(**params)
        if keyword:
            cr_q = self.query_keyword(cr_q, keyword,
                                      "name", "desc", "db_type", "summary")
        ret, p = self.paginate(cr_q, **p)
        self.resp([i.to_dict() for i in ret], **p)

    def post(self):
        """新增库规则"""
        params = self.get_json_args(Schema({
            "cmdb_id": scm_gt0_int,
            **self.base_rule_schema_for_adding()
        }))
        new_cr = CMDBRule(**params)
        self.test_code(new_cr)
        new_cr.save()
        self.resp_created(new_cr.to_dict())

    def put(self):
        """通用修改库规则"""
        params = self.get_json_args(Schema({
            "cmdb_id": scm_gt0_int,
            "name": scm_unempty_str,

            **self.base_rule_schema_for_whole_updating()
        }))
        cmdb_id = params.pop("cmdb_id")
        name = params.pop("name")
        the_cr = CMDBRule.objects(cmdb_id=cmdb_id, name=name).first()
        the_cr.from_dict(params)
        self.test_code(the_cr)
        the_cr.save()
        self.resp_created(the_cr.to_dict())

    def patch(self):
        """修改库规则(输入参数，输出参数，入口)"""
        params = self.get_json_args(Schema({
            "cmdb_id": scm_gt0_int,
            "name": scm_unempty_str,

            **self.base_rule_schema_for_special_updating()
        }))
        cmdb_id = params.pop("cmdb_id")
        name = params.pop("name")
        the_cr = CMDBRule.objects(cmdb_id=cmdb_id, name=name).first()
        self.special_update(the_cr, **params)
        self.save()
        self.resp_created(the_cr.to_dict())

    def delete(self):
        """删除库规则"""
        params = self.get_json_args(Schema({
            "cmdb_id": scm_gt0_int,
            "name": scm_unempty_str
        }))
        the_cr = CMDBRule.objects(**params).first()
        the_cr.delete()
        self.resp_created(msg="已删除")
