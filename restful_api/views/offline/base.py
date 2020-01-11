# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "TicketReq"
]

from typing import Union

from schema import Schema
from mongoengine import QuerySet as mongoengine_qs
from sqlalchemy import or_
from sqlalchemy.orm import Query as sqlalchemy_qs

from models.oracle import WorkList
from restful_api.views.base import PrivilegeReq
from utils.const import *
from utils.schema_utils import *


class TicketReq(PrivilegeReq):
    """工单通用请求，提供权限过滤"""

    def __init__(self, *args, **kwargs):
        super(TicketReq, self).__init__(*args, **kwargs)
        self.db_type = None
        self.db_type_schema = Schema({
            "db_type": scm_one_of_choices(ALL_SUPPORTED_DB_TYPE),
            scm_optional(object): object
        })

    def privilege_filter_ticket(self, q: sqlalchemy_qs) -> sqlalchemy_qs:
        """根据登录用户的权限过滤工单"""
        if self.has(PRIVILEGE.PRIVILEGE_OFFLINE_TICKET_ADMIN):
            # 超级权限，可看所有的工单
            pass

        elif self.has(PRIVILEGE.PRIVILEGE_OFFLINE_TICKET_APPROVAL):
            # 能看:自己提交的工单+指派给自己所在角色的工单+自己处理了的工单
            q = q.filter(
                or_(
                    WorkList.submit_owner == self.current_user,
                    WorkList.audit_role_id.in_(self.current_roles()),
                    WorkList.audit_owner == self.current_user
                )
            )

        else:
            # 只能看:自己提交的工单
            q = q.filter(WorkList.submit_owner == self.current_user)
        return q

    def privilege_filter_sub_ticket(self, q: mongoengine_qs, session) -> mongoengine_qs:
        """根据登录用户的权限过滤子工单"""
        if self.has(PRIVILEGE.PRIVILEGE_OFFLINE_TICKET_ADMIN):
            # 超级权限，可看所有子工单
            pass

        elif self.has(PRIVILEGE.PRIVILEGE_OFFLINE_TICKET_APPROVAL):
            # 能看:自己提交的子工单+指派给自己所在角色的子工单+自己处理过了的工单
            sq = session.query(WorkList.work_list_id). \
                filter(or_(
                WorkList.submit_owner == self.current_user,
                WorkList.audit_role_id.in_(self.current_roles()),
                WorkList.audit_owner == self.current_user
            ))
            ticket_ids: list = list(sq)
            q = q.filter(work_list_id__in=ticket_ids)

        else:
            # 只能看:自己提交的子工单
            sq = session.query(WorkList.work_list_id). \
                filter(WorkList.submit_owner == self.current_user)
            ticket_ids: list = list(sq)
            q = q.filter(work_list_id__in=ticket_ids)

        return q

    def get_query_args_according_to_db_type(
            self, *args, **kwargs) -> Union[dict, None]:
        """query args: 去除db_type字段，把db_type保存在self里"""
        params = super(TicketReq, self).get_query_args(self.db_type_schema)
        self.db_type = params.pop("db_type")
        return super(TicketReq, self).get_query_args(kwargs[self.db_type])

    def get_json_args_according_to_db_type(
            self, *args, **kwargs) -> Union[dict, list, None]:
        """body json: 去除db_type字段，把db_type保存在self里"""
        params = super(TicketReq, self).get_json_args(self.db_type_schema)
        self.db_type = params.pop("db_type")
        return super(TicketReq, self).get_json_args(kwargs[self.db_type])
