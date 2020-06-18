# Author: kk.Fang(fkfkbill@gmail.com)

from mongoengine import NotUniqueError

from restful_api.modules import *
from utils.schema_utils import *
from ..cmdb_rule import CMDBRule
from .base import *


@as_view(group="cmdb-rule")
class CMDBRuleHandler(BaseRuleHandler):

    def get(self):
        """库规则列表"""
        params = self.get_query_args(Schema({
            "cmdb_id": scm_gt0_int,
            scm_optional("status", default=None): scm_empty_as_optional(scm_bool),

            scm_optional("keyword", default=None): scm_str,
            **self.gen_p()
        }))
        keyword = params.pop("keyword")
        status = params.pop("status")
        p = self.pop_p(params)

        cr_q = CMDBRule.filter(**params)
        if keyword:
            cr_q = self.query_keyword(cr_q, keyword,
                                      "name", "desc", "db_type", "summary")
        if status is not None:
            cr_q = cr_q.filter(status=status)
        ret, p = self.paginate(cr_q, **p)
        self.resp([i.to_dict() for i in ret], **p)

    get.argument = {
        "querystring": {
            "cmdb_id": 2526,
            "//status": "1",
            "//keyword": "em",
            "//page": 1,
            "//per_page": 10
        }
    }

    def post(self):
        """新增库规则"""
        params = self.get_json_args(Schema({
            "cmdb_id": scm_gt0_int,
            **self.base_rule_schema_for_adding()
        }))
        new_cr = CMDBRule(**params)
        self.test_code(new_cr)
        try:
            new_cr.save()
        except NotUniqueError:
            return self.resp_bad_req(msg=f"{new_cr.unique_key()} 已存在")
        self.resp_created(new_cr.to_dict())

    post.argument = {
        "json": {
            "cmdb_id": 2526,

            "desc": "",
            "db_type": "oracle",
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
        """通用修改库规则"""
        params = self.get_json_args(Schema({
            "cmdb_id": scm_gt0_int,
            "name": scm_unempty_str,

            **self.base_rule_schema_for_whole_updating()
        }))
        cmdb_id = params.pop("cmdb_id")
        name = params.pop("name")
        the_cr = CMDBRule.filter(cmdb_id=cmdb_id, name=name).first()
        the_cr.from_dict(params)
        self.test_code(the_cr)
        the_cr.save()
        self.resp_created(the_cr.to_dict())

    put.argument = {
        "json": {
            "cmdb_id": 2526,
            "name": "emm"

        }
    }

    def patch(self):
        """修改库规则(输入参数，输出参数，entries)"""
        params = self.get_json_args(Schema({
            "cmdb_id": scm_gt0_int,
            "name": scm_unempty_str,

            **self.base_rule_schema_for_special_updating()
        }))
        cmdb_id = params.pop("cmdb_id")
        name = params.pop("name")
        the_cr = CMDBRule.filter(cmdb_id=cmdb_id, name=name).first()
        self.special_update(the_cr, **params)
        self.save()
        self.resp_created(the_cr.to_dict())

    def delete(self):
        """删除库规则"""
        params = self.get_json_args(Schema({
            "cmdb_id": scm_gt0_int,
            "name": scm_unempty_str
        }))
        the_cr = CMDBRule.filter(**params).first()
        if not the_cr:
            return self.resp_bad_req(msg=f"{params}规则未找到")
        the_cr.delete()
        self.resp_created(msg="已删除")

    delete.argument = {
        "json": {
            "cmdb_id": 2526,
            "name": "emmm"
        }
    }
