# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "TicketReq"
]

import re
import abc

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


class SubTicketAnalysis(abc.ABC):
    """子工单分析模块，不指明纳管库类型"""

    # TODO 指明数据库类型，mysql还是oracle
    db_type = None

    def get_available_task_name(self, submit_owner: str) -> str:
        """获取当前可用的线下审核任务名"""
        current_date = d_to_str(arrow.now().date(), fmt=COMMON_DATE_FORMAT_COMPACT)
        k = f"offline-ticket-task-num-{current_date}"
        current_num = "%03d" % self.redis_cli.incr(k, 1)
        self.redis_cli.expire(k, 60 * 60 * 24 * 3)  # 设置三天内超时
        return f"{submit_owner}-{current_date}-{current_num}"

    @abc.abstractmethod
    def __init__(self,
                 static_rules_qs: mongoengine_qs,
                 dynamic_rules_qs: mongoengine_qs,
                 cmdb: CMDB,
                 ticket: WorkList):
        # 缓存存放每日工单的子增流水号
        self.redis_cli = StrictRedis(
            host=settings.CACHE_REDIS_IP,
            port=settings.CACHE_REDIS_PORT,
            db=settings.CACHE_REDIS_DB
        )
        # 存放规则快照，当前工单在分析的时候，每一条语句都用这个规则快照去分析
        # 如果语句很长，分析过程中如果有人修改了线下规则，则同一个工单里不同语句的依据标准不一样
        # 另一个是，每次都产生新的规则对象，会重新构建规则代码，耗时没意义
        # TODO 重载初始化函数，指明是使用oracle还是mysql的规则
        self.static_rules = list(static_rules_qs)
        self.dynamic_rules = list(dynamic_rules_qs)
        self.cmdb = cmdb
        self.ticket = ticket
        self.cmdb_connector = None

    @staticmethod
    def sql_filter_annotation(sql):
        """用于去掉每句sql末尾的分号(目前仅oracle需要这么做)"""
        if not sql:
            return ""
        sql = sql[:-1] if sql and sql[-1] == ";" else sql
        return sql.strip()

    def run_static(
            self,
            sub_result,
            sqls: [dict],
            single_sql: dict):
        """
        静态分析
        :param self:
        :param sub_result:
        :param single_sql: {"sql_text":,"comments":,"num":}
        :param sqls: [{single_sql},...]
        """
        for sr in self.static_rules:
            sub_result_item = TicketSubResultItem()
            sub_result_item.as_sub_result_of(sr)

            # ===这里指明了静态审核的输入参数(kwargs)===
            ret = sr.analyse(
                single_sql=single_sql,
                sqls=sqls
            )

            for output, current_ret in zip(sr.output_params, ret):
                sub_result_item.add_output(**{
                    **output,
                    "value": current_ret
                })
            sub_result_item.calc_score()
            sub_result.static.append(sub_result_item)

    @abc.abstractmethod
    def run_dynamic(self, sub_result, single_sql: dict):
        """动态分析"""
        pass

    @abc.abstractmethod
    def write_sql_plan(self, **kwargs):
        """写入动态检测到的执行计划"""
        pass

    @abc.abstractmethod
    def run(
            self,
            sqls: [dict],
            single_sql: dict,
            **kwargs
    ):
        """单条sql语句的分析"""
        pass
