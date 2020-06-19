# Author: kk.Fang(fkfkbill@gmail.com)

from mongoengine import NotUniqueError

import auth.const
from utils.schema_utils import *
from restful_api import *
from auth.restful_api.base import *
from ..audit_process import *


@as_view(group="ticket")
class AuditProcessTemplateHandler(PrivilegeReq):

    def get(self):
        """工单审核流程模板列表"""
        params = self.get_query_args(Schema({
            **self.gen_p()
        }))
        p = self.pop_p(params)
        tmpl_q = TicketAuditProcessTemplate.filter()
        content, p = self.paginate(tmpl_q, **p)
        self.resp([i.to_dict() for i in content], **p)

    get.argument = {
        "querystring": {
            "//page": 1,
            "//per_page": 10
        }
    }

    def post(self):
        """工单审核流程模板新增"""
        self.acquire(auth.const.PRIVILEGE.PRIVILEGE_TICKET_AUDIT_PROCESS_EDIT)

        params = self.get_json_args(Schema({
            "name": scm_unempty_str,
            "process": scm_and(
                scm_deduplicated_list_of_dict,
                [{
                    "audit_role_id": scm_int,
                    "audit_role_name": scm_unempty_str
                }]
            )
        }))
        process = params.pop("process")
        new_tmpl = TicketAuditProcessTemplate(**params)
        for a_process in process:
            new_tmpl.process.append(TicketManualAudit(
                **a_process
            ))
        try:
            new_tmpl.save()
        except NotUniqueError:
            return self.resp_bad_req(msg="模板名称重复。")
        self.resp(new_tmpl.to_dict())

    post.argument = {
        "json": {
            "name": "新审核模板",
            "process": [
                {
                    "audit_role_id": 1,
                    "audit_role_name": "emmm"
                },
                {
                    "audit_role_id": 2,
                    "audit_role_name": "emmmm"
                }
            ]
        }
    }

    def patch(self):
        """工单审核流程模板编辑"""
        self.acquire(auth.const.PRIVILEGE.PRIVILEGE_TICKET_AUDIT_PROCESS_EDIT)

        params = self.get_json_args(Schema({
            "id": scm_unempty_str,

            scm_optional("name"): scm_unempty_str,
            scm_optional("process"): scm_and(
                scm_deduplicated_list_of_dict,
                [{
                    "audit_role_id": scm_int,
                    "audit_role_name": scm_unempty_str
                }]
            )
        }))
        the_id = params.pop("_id")
        process = params.pop("process") if "process" in params.keys() else None
        the_tmpl = TicketAuditProcessTemplate.filter(_id=the_id).first()
        if not the_tmpl:
            return self.resp_bad_req(msg="模板不存在。")
        the_tmpl.from_dict(params)
        if process is not None:
            the_tmpl.process.clear()
            for a_process in process:
                the_tmpl.process.append(TicketManualAudit(
                    **a_process
                ))
        try:
            the_tmpl.save()
        except NotUniqueError:
            return self.resp_bad_req(msg="模板名称重复。")
        self.resp(the_tmpl.to_dict())

    patch.argument = {
        "json": {
            "id": "ewfowngo",
            "name": "新审核模板",
            "process": [
                {
                    "audit_role_id": 1,
                    "audit_role_name": "emmm"
                },
                {
                    "audit_role_id": 2,
                    "audit_role_name": "emmmm"
                }
            ]
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
