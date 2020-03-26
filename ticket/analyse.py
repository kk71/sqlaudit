# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "SubTicketAnalyse"
]

import abc
import traceback

from prettytable import PrettyTable
from redis import StrictRedis
from mongoengine import QuerySet as mongoengine_qs

import settings
import new_rule.const
from models.oracle import CMDB
from . import const
from utils.datetime_utils import *
from plain_db.mongo_operat import MongoHelper
from .ticket import Ticket
from .sub_ticket import SubTicketIssue


class SubTicketAnalyse(abc.ABC):
    """子工单分析模块，不指明纳管库类型"""

    # TODO 指明数据库类型，mysql还是oracle
    db_type = None

    def get_available_task_name(self, submit_owner: str) -> str:
        """获取当前可用的线下审核任务名"""
        current_date = d_to_str(arrow.now().date(), fmt=COMMON_DATE_FORMAT_COMPACT)
        k = f"ticket-task-num-{current_date}"
        current_num_int = self.redis_cli.incr(k, 1)
        current_num = "%03d" % current_num_int
        self.redis_cli.expire(k, 60 * 60 * 24 * 3)  # 设置三天内超时
        ret = f"{submit_owner}-{current_date}-{current_num}"
        if current_num_int == 1:
            while Ticket.objects(task_name=ret).count():
                current_num_int = self.redis_cli.incr(k, 1)
                current_num = "%03d" % current_num_int
                self.redis_cli.expire(k, 60 * 60 * 24 * 3)  # 设置三天内超时
                ret = f"{submit_owner}-{current_date}-{current_num}"
        return ret

    @abc.abstractmethod
    def __init__(self,
                 static_rules_qs: mongoengine_qs,
                 dynamic_rules_qs: mongoengine_qs,
                 cmdb: CMDB,
                 ticket: Ticket):
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
        self.mongo_connector = MongoHelper.get_db()

    @staticmethod
    def update_error_message(error_type, msg, trace=None, old_msg="") -> str:
        """
        格式化更新报错信息
        :param error_type: 错误类型中问提示
        :param msg: 当前的报错信息
        :param trace: 堆栈信息可选
        :param old_msg: 原先的错误信息，用以拼接
        :return:
        """
        pt = PrettyTable([error_type])
        if trace:
            msg += "\n\n" + trace
        pt.add_row([msg])
        if not old_msg:
            old_msg = ""
        return old_msg + "\n\n" + str(pt)

    def run_static(
            self,
            sub_result,
            sqls: [dict],
            single_sql: dict):
        """
        静态分析
        :param self:
        :param sub_result:
        :param single_sql:
        :param sqls: [{single_sql},...]
        """
        try:
            for sr in self.static_rules:
                if sr.sql_type is not const.SQL_ANY and \
                        sr.sql_type != single_sql["sql_type"]:
                    continue
                sub_result_issue = SubTicketIssue()
                sub_result_issue.as_sub_result_of(sr)

                # ===指明静态审核的输入参数(kwargs)===
                score_to_minus, output_params = sr.run(
                    entry=new_rule.const.RULE_ENTRY_TICKET_STATIC,

                    single_sql=single_sql,
                    sqls=sqls,
                    cmdb=self.cmdb
                )
                for output, current_ret in zip(sr.output_params, output_params):
                    sub_result_issue.add_output(output, current_ret)
                sub_result_issue.minus_score = score_to_minus
                if sub_result_issue.minus_score != 0:
                    sub_result.static.append(sub_result_issue)
        except Exception as e:
            error_msg = str(e)
            trace = traceback.format_exc()
            sub_result.error_msg = self.update_error_message(
                "静态审核", msg=error_msg, trace=trace, old_msg=sub_result.error_msg)
            print(error_msg)
            print(trace)

    @abc.abstractmethod
    def run_dynamic(self, sub_result, single_sql: dict):
        """动态分析"""
        pass

    @abc.abstractmethod
    def write_sql_plan(self, **kwargs) -> mongoengine_qs:
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

    @abc.abstractmethod
    def sql_online(self, sql, **kwargs):
        pass
