# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "TicketReq"
]

from auth.const import PRIVILEGE
from models.mongoengine import *
from utils.schema_utils import *
from ..ticket import Ticket, const
from auth.restful_api.base import *


class TicketReq(PrivilegeReq):
    """工单通用请求，提供权限过滤"""

    def __init__(self, *args, **kwargs):
        super(TicketReq, self).__init__(*args, **kwargs)
        self.scm_status = self.scm_or_with_error_msg(
            And(scm_int, scm_one_of_choices(const.ALL_TICKET_STATUS)),
            e=f"工单规则为：{const.ALL_TICKET_STATUS}"
        )

    def privilege_filter_ticket(self, q: mongoengine_qs) -> mongoengine_qs:
        """根据登录用户的权限过滤工单"""
        if self.has(PRIVILEGE.PRIVILEGE_OFFLINE_TICKET_ADMIN):
            # 超级权限，可看所有的工单
            pass

        elif self.has(PRIVILEGE.PRIVILEGE_OFFLINE_TICKET_APPROVAL):
            # 能看:自己提交的工单+指派给自己所在角色的工单+自己处理了的工单
            q = q.filter(
                Q(submit_owner=self.current_user) |
                Q(audit_role_id__in=self.current_roles()) |
                Q(audit_owner=self.current_user)
            )

        else:
            # 只能看:自己提交的工单
            q = q.filter(submit_owner=self.current_user)

        return q

    def privilege_filter_sub_ticket(self, q: mongoengine_qs) -> mongoengine_qs:
        """根据登录用户的权限过滤子工单"""
        if self.has(PRIVILEGE.PRIVILEGE_OFFLINE_TICKET_ADMIN):
            # 超级权限，可看所有子工单
            pass

        elif self.has(PRIVILEGE.PRIVILEGE_OFFLINE_TICKET_APPROVAL):
            # 能看:自己提交的子工单+指派给自己所在角色的子工单+自己处理过了的工单
            sq = Ticket.objects(
                Q(submit_owner=self.current_user) |
                Q(audit_role_id__in=self.current_roles()) |
                Q(audit_owner=self.current_user)
            ).values_list("ticket_id")
            ticket_ids: list = list(sq)
            q = q.filter(ticket_id__in=ticket_ids)

        else:
            # 只能看:自己提交的子工单
            sq = Ticket.objects(
                submit_owner=self.current_user).values_list("ticket_id")
            ticket_ids: list = list(sq)
            q = q.filter(ticket_id__in=ticket_ids)

        return q
