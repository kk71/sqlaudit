# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "TicketReq"
]

from mongoengine import QuerySet as mongoengine_qs
from sqlalchemy import or_
from sqlalchemy.orm import Query as sqlalchemy_qs

from models.oracle import WorkList
from restful_api.views.base import PrivilegeReq
from utils.const import PRIVILEGE


class TicketReq(PrivilegeReq):
    """工单通用请求，提供权限过滤"""

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