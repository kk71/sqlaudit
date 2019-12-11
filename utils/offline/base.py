# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OfflineTicketCommonHandler",
    "SubTicketAnalysis",
]

import re
import uuid

import sqlparse
from redis import StrictRedis
from sqlalchemy import or_
from mongoengine import QuerySet as mongoengine_qs
from sqlalchemy.orm.query import Query as sqlalchemy_qs

import settings
from models.mongo import *
from models.oracle import CMDB, WorkList
from plain_db.oracleob import *
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
    """子工单分析模块，不指明纳管库类型"""

    @staticmethod
    def get_available_task_name(submit_owner: str, sql_type: int) -> str:
        """获取当前可用的线下审核任务名"""
        current_date = d_to_str(arrow.now().date(), fmt=COMMON_DATE_FORMAT_COMPACT)
        k = f"offline-ticket-task-num-{current_date}"
        current_num = "%03d" % cache_redis_cli.incr(k, 1)
        cache_redis_cli.expire(k, 60 * 60 * 24 * 3)  # 设置三天内超时
        return f"{submit_owner}-{ALL_SQL_TYPE_NAME_MAPPING[sql_type]}-" \
               f"{current_date}-{current_num}"

    def __init__(self,
                 static_rules_qs: mongoengine_qs,
                 dynamic_rules_qs: mongoengine_qs):
        # 存放规则快照，当前工单在分析的时候，每一条语句都用这个规则快照去分析
        # 如果语句很长，分析过程中如果有人修改了线下规则，则同一个工单里不同语句的依据标准不一样
        # 另一个是，每次都产生新的规则对象，会重新构建规则代码，耗时没意义

        # TODO 重载初始化函数，指明是使用oracle还是mysql的规则
        self.static_rules = list(static_rules_qs)
        self.dynamic_rules = list(dynamic_rules_qs)

    @classmethod
    def sql_filter_annotation(cls, sql):
        """老代码挪过来的，主要是用于去掉每句sql末尾的分号"""
        if not sql:
            return ""

        sql = sql[:-1] if sql and sql[-1] == ";" else sql

        return sql.strip()

    @classmethod
    def judge_sql_type(cls, sql_text: str) -> int:
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

    def run_static(
            self,
            sub_result: TicketSubResult,
            single_sql: str):
        """
        静态分析
        :param self:
        :param sub_result:
        :param single_sql:
        """
        for sr in self.static_rules:
            sub_result_item = TicketSubResultItem()
            sub_result_item.as_sub_result_of(sr)
            formatted_sql = sqlparse.format(single_sql, strip_whitespace=True).lower()
            ret = sr.analyse(formatted_sql)
            if not isinstance(ret, (list, tuple)):
                raise RuleCodeInvalidException("The data ticket rule returned "
                                               f"is not a list or tuple: {ret}")
            if len(ret) != len(sr.output_params):
                raise RuleCodeInvalidException(
                    f"The length of the iterable ticket rule returned({len(ret)}) "
                    f"is not equal with defined in rule({len(sr.output_params)})")
            for output, current_ret in zip(sr.output_params, ret):
                sub_result_item.add_output(**{
                    **output,
                    "value": current_ret
                })
            sub_result_item.calc_score()
            sub_result.static.append(sub_result_item)

    def run_dynamic(self, sub_result: TicketSubResult, single_sql: str):
        """动态分析"""
        raise NotImplementedError

    def run(
            self,
            session,
            work_list_id: int,
            cmdb: CMDB,
            schema: str,
            single_sql: str,
            position: int) -> TicketSubResult:
        """
        单条sql的静态动态审核
        :param session:
        :param work_list_id:
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
        # 动态分析
        for dr in self.dynamic_rules:
            statement_id = uuid.uuid1().hex
            sub_result_item = TicketSubResultItem()
            sub_result_item.as_sub_result_of(dr)
            cmdb_connector = OracleCMDBConnector(cmdb)
            formatted_sql = self.sql_filter_annotation(single_sql)
            cmdb_connector.execute(f"alter session set current_schema={schema}")
            cmdb_connector.execute("EXPLAIN PLAN SET "
                                   f"statement_id='{statement_id}' for {formatted_sql}")
            sql_for_getting_plan = f"SELECT * FROM plan_table " \
                                   f"WHERE statement_id = '{statement_id}'"
            sql_plans = cmdb_connector.select_dict(sql_for_getting_plan, one=False)
            cmdb_connector.close()
            if not sql_plans:
                raise Exception(f"fatal: No plans for "
                                f"statement_id: {statement_id}, sql: {formatted_sql}")
            OracleTicketSQLPlan.add_from_dict(
                work_list_id, cmdb.cmdb_id, schema, sql_plans)

        return sub_result

