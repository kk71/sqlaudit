# Author: kk.Fang(fkfkbill@gmail.com)

from mongoengine import NotUniqueError

import auth.const
import auth.user
from models.sqlalchemy import *
from utils.schema_utils import *
from restful_api import *
from auth.restful_api.base import *
from ..audit_process import *


@as_view(group="ticket")
class AuditProcessTemplateHandler(PrivilegeReq):

    def get(self):
        """工单审核流程模板列表"""
        params = self.get_query_args(Schema({
            scm_optional("keyword", default=None): scm_empty_as_optional(scm_str),
            **self.gen_p()
        }))
        p = self.pop_p(params)
        keyword = params.pop("keyword")
        tmpl_q = TicketAuditProcessTemplate.filter()
        if keyword:
            tmpl_q = self.query_keyword(tmpl_q, keyword, "name")
        content, p = self.paginate(tmpl_q, **p)
        self.resp([i.to_dict() for i in content], **p)

    get.argument = {
        "querystring": {
            "//page": 1,
            "//per_page": 10,
            "//keyword": "emm"
        }
    }

    def post(self):
        """工单审核流程模板新增"""
        self.acquire(auth.const.PRIVILEGE.PRIVILEGE_TICKET_AUDIT_PROCESS_EDIT)

        params = self.get_json_args(Schema({
            "name": scm_unempty_str,
            "process": scm_deduplicated_list
        }))
        process = params.pop("process")
        new_tmpl = TicketAuditProcessTemplate(**params)
        with make_session() as session:
            for a_process in process:
                the_role = session.query(auth.user.Role).filter_by(
                    role_id=a_process).first()
                if not the_role:
                    continue
                new_tmpl.process.append(TicketManualAudit(
                    audit_role_id=the_role.role_id,
                    audit_role_name=the_role.role_name
                ))
        if not new_tmpl.process.count():
            return self.resp_bad_req(msg="没有选中任何角色")
        try:
            new_tmpl.save()
        except NotUniqueError:
            return self.resp_bad_req(msg="模板名称重复。")
        self.resp(new_tmpl.to_dict())

    post.argument = {
        "json": {
            "name": "新审核模板",
            "process": [
                1
            ]
        }
    }

    def patch(self):
        """工单审核流程模板编辑"""
        self.acquire(auth.const.PRIVILEGE.PRIVILEGE_TICKET_AUDIT_PROCESS_EDIT)

        params = self.get_json_args(Schema({
            "id": scm_unempty_str,

            scm_optional("name"): scm_unempty_str,
            scm_optional("process"): scm_deduplicated_list
        }))
        the_id = params.pop("id")
        process = params.pop("process") if "process" in params.keys() else None
        the_tmpl = TicketAuditProcessTemplate.filter(_id=the_id).first()
        if not the_tmpl:
            return self.resp_bad_req(msg="模板不存在。")
        the_tmpl.from_dict(params)
        if process is not None:
            with make_session() as session:
                the_tmpl.process.clear()
                for a_process in process:
                    the_role = session.query(auth.user.Role).filter_by(
                        role_id=a_process).first()
                    if not the_role:
                        continue
                    the_tmpl.process.append(TicketManualAudit(
                        audit_role_id=the_role.role_id,
                        audit_role_name=the_role.role_name
                    ))
        if not the_tmpl.process.count():
            return self.resp_bad_req(msg="没有选中任何角色")
        try:
            the_tmpl.save()
        except NotUniqueError:
            return self.resp_bad_req(msg="模板名称重复。")
        self.resp(the_tmpl.to_dict())

    patch.argument = {
        "json": {
            "id": "ewfowngo",
            "name": "新审核模板",
            "process": [1]
        }
    }

    def delete(self):
        """工单审核流程模板删除"""
        self.acquire(auth.const.PRIVILEGE.PRIVILEGE_TICKET_AUDIT_PROCESS_EDIT)

        params = self.get_json_args(Schema({
            "id": scm_unempty_str,
        }))
        the_tmpl = TicketAuditProcessTemplate.filter(**params).first()
        if not the_tmpl:
            return self.resp_bad_req(msg="模板不存在。")
        the_tmpl.delete()
        self.resp_created()

    delete.argument = {
        "json": {
            "id": "asfewfgwnwih"
        }
    }
