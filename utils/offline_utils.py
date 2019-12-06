# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OfflineTicketCommonHandler",
    "analyse_sql"
]

from redis import StrictRedis
from sqlalchemy import or_
from mongoengine import QuerySet as mongoengine_qs
from sqlalchemy.orm.query import Query as sqlalchemy_qs

import settings
from models.mongo import *
from models.oracle import WorkList
from utils.const import *
from utils.datetime_utils import *
from restful_api.views.base import PrivilegeReq


cache_redis_cli = StrictRedis(
    host=settings.CACHE_REDIS_IP,
    port=settings.CACHE_REDIS_PORT,
    db=settings.CACHE_REDIS_DB
)


class OfflineTicketCommonHandler(PrivilegeReq):

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


def get_current_offline_ticket_task_name(submit_owner, sql_type):
    """获取当前可用的线下审核任务名"""
    current_date = d_to_str(arrow.now().date(), fmt=COMMON_DATE_FORMAT_COMPACT)
    k = f"offline-ticket-task-num-{current_date}"
    current_num = "%03d" % cache_redis_cli.incr(k, 1)
    cache_redis_cli.expire(k, 60*60*24*3)  # 设置三天内超时
    return f"{submit_owner}-{ALL_SQL_TYPE_NAME_MAPPING[sql_type]}-" \
           f"{current_date}-{current_num}"


def judge_sql_type(sql_text: str):
    """
    判断单条语句是DDL还是DML
    :param sql_text:
    :return:
    """
    return SQL_DDL if 'create' in sql_text or \
                      'drop' in sql_text or \
                      'alter' in sql_text else SQL_DML


def analyse_sql(work_list_id: int):
    """
    分析线下审核
    :param work_list_id:
    :return:
    """
    return
