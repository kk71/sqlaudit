# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OfflineTicketCommonHandler",
    "SubTicketAnalysis",
]

import re

from redis import StrictRedis
from sqlalchemy import or_
from mongoengine import QuerySet as mongoengine_qs
from sqlalchemy.orm.query import Query as sqlalchemy_qs

import settings
from models.mongo import *
from models.oracle import CMDB, WorkList
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


class SubTicketAnalysis:

    def __init__(self, rules_qs: mongoengine_qs = None):
        # 存放规则快照，当前工单在分析的时候，每一条语句都用这个规则快照去分析
        # 如果语句很长，分析过程中如果有人修改了线下规则，则同一个工单里不同语句的依据标准不一样
        # 另一个是，每次都产生新的规则对象，会重新构建规则代码，耗时没意义
        if rules_qs is None:
            rules_qs = TicketRule.filter_enabled()
        self.rules = list(rules_qs)

    def run(
            self,
            session,
            cmdb: CMDB,
            schema: str,
            single_sql: str,
            position: int) -> TicketSubResult:
        """
        单条sql的静态动态审核
        :param session:
        :param cmdb:
        :param schema:
        :param single_sql:
        :param position: 当前语句在整个工单里的位置，从0开始计
        """
        sub_result = TicketSubResult(
            cmdb_id=cmdb.cmdb_id,
            schema_name=schema,
            position=position,
            sql_text=single_sql
        )
        for t_rule in self.rules:
            t_rule.analyse()
        return sub_result


def get_current_offline_ticket_task_name(submit_owner, sql_type):
    """获取当前可用的线下审核任务名"""
    current_date = d_to_str(arrow.now().date(), fmt=COMMON_DATE_FORMAT_COMPACT)
    k = f"offline-ticket-task-num-{current_date}"
    current_num = "%03d" % cache_redis_cli.incr(k, 1)
    cache_redis_cli.expire(k, 60*60*24*3)  # 设置三天内超时
    return f"{submit_owner}-{ALL_SQL_TYPE_NAME_MAPPING[sql_type]}-" \
           f"{current_date}-{current_num}"


def judge_sql_type(sql_text: str) -> int:
    """
    判断单条语句是DDL还是DML
    :param sql_text: 单条sql语句
    :return:
    """
    if re.match(r"select\s+|delete\s+|insert\s+|update\s+",
                sql_text, re.I):
        return SQL_DML
    elif re.match(r"create\s+|alter\s+|drop\s+|truncate\s+|grant\s+|revoke\s+",
                  sql_text, re.I):
        return SQL_DDL
    else:
        assert 0
