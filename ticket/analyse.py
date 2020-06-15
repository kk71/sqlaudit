# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "SubTicketAnalyseStaticCMDBIndependent",
    "SubTicketAnalyse"
]

import abc
import copy
import traceback
from typing import List

from prettytable import PrettyTable

from models.mongoengine import *
from .ticket import Ticket
from .sub_ticket import SubTicketIssue
from rule.rule_jar import *
from rule.adapters import *
from parsed_sql.single_sql import *
from .single_sql import SingleSQLForTicket


class BaseSubTicketAnalyse(abc.ABC):
    """基础子工单分析"""

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


class BaseSubTicketAnalyseStatic(BaseSubTicketAnalyse):
    """基础子工单分析"""

    def __init__(self, static_rules: BaseRuleJar):
        self.static_rules: RuleJar = static_rules

    @abc.abstractmethod
    def run_static(
            self,
            sub_result,
            sqls: [dict],
            single_sql: dict):
        pass


class BaseSubTicketAnalyseDynamic(BaseSubTicketAnalyse):

    def __init__(self, dynamic_rules: RuleJar):
        self.dynamic_rules: RuleJar = dynamic_rules

    @abc.abstractmethod
    def run_dynamic(self, sub_result, single_sql: dict):
        """动态分析"""
        pass

    @abc.abstractmethod
    def write_sql_plan(self, **kwargs) -> mongoengine_qs:
        """写入动态检测到的执行计划"""
        pass


class SubTicketAnalyseStaticCMDBIndependent(BaseSubTicketAnalyseStatic):
    """一个与库无关的静态分析子工单"""

    def run_static(
            self,
            sub_result,
            sqls: List[SingleSQLForTicket],
            single_sql: SingleSQLForTicket):
        """
        静态分析
        :param self:
        :param sub_result:
        :param single_sql:
        :param sqls: [{single_sql},...]
        """
        try:
            for sr in self.static_rules:
                if single_sql["sql_type"] not in sr.entries:
                    # 这里默认sql type和规则entries的类型在文本层面是相等的
                    # 实际都是文本，注意发生更改需要修改
                    continue
                # ===指明静态审核的输入参数(kwargs)===
                ret = RuleAdapterSQL(sr).run(
                    entries=self.static_rules.entries,

                    single_sql=copy.copy(single_sql),
                    sqls=sqls
                )
                for minus_score, output_param in ret:
                    sub_result_issue = SubTicketIssue()
                    sub_result_issue.as_issue_of(
                        sr,
                        output_data=output_param,
                        minus_score=minus_score
                    )
                    sub_result.static.append(sub_result_issue)
        except Exception as e:
            error_msg = str(e)
            trace = traceback.format_exc()
            sub_result.error_msg = self.update_error_message(
                "静态审核", msg=error_msg, trace=trace, old_msg=sub_result.error_msg)
            print(error_msg)
            print(trace)


class SubTicketAnalyse(
        BaseSubTicketAnalyseDynamic,
        BaseSubTicketAnalyseStatic):
    """子工单分析模块，不指明纳管库类型"""

    def __init__(self,
                 static_rules: RuleJar,
                 dynamic_rules: RuleJar,
                 cmdb,
                 ticket: Ticket):
        # 缓存存放每日工单的子增流水号
        BaseSubTicketAnalyseStatic.__init__(self, static_rules)
        BaseSubTicketAnalyseDynamic.__init__(self, dynamic_rules)
        self.cmdb = cmdb
        self.ticket = ticket
        self.cmdb_connector = None

    def run_static(
            self,
            sub_result,
            sqls: [SingleSQL],
            single_sql: SingleSQL):
        """
        静态分析
        :param self:
        :param sub_result:
        :param single_sql:
        :param sqls: [{single_sql},...]
        """
        print(single_sql)
        try:
            for sr in self.static_rules:
                if single_sql["sql_type"] not in sr.entries:
                    # 这里默认sql type和规则entries的类型在文本层面是相等的
                    # 实际都是文本，注意发生更改需要修改
                    continue
                # ===指明静态审核的输入参数(kwargs)===
                ret = RuleAdapterSQL(sr).run(
                    entries=self.static_rules.entries,

                    single_sql=copy.copy(single_sql),
                    sqls=copy.copy(sqls),
                    cmdb=self.cmdb
                )
                for minus_score, output_param in ret:
                    sub_result_issue = SubTicketIssue()
                    sub_result_issue.as_issue_of(
                        sr,
                        output_data=output_param,
                        minus_score=minus_score
                    )
                    sub_result.static.append(sub_result_issue)
        except Exception as e:
            error_msg = str(e)
            trace = traceback.format_exc()
            sub_result.error_msg = self.update_error_message(
                "静态审核", msg=error_msg, trace=trace, old_msg=sub_result.error_msg)
            print(error_msg)
            print(trace)

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

