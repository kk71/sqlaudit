# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleSubTicketAnalysis",
]

import uuid
import traceback

from mongoengine import QuerySet as mongoengine_qs

from models.mongo import *
from plain_db.oracleob import *
from utils.const import *
from .base import SubTicketAnalysis


class OracleSubTicketAnalysis(SubTicketAnalysis):
    """oracle子工单分析模块"""

    db_type = DB_ORACLE

    def __init__(self, **kwargs):
        if kwargs.get("static_rules_qs", None) is None:
            kwargs["static_rules_qs"] = TicketRule.filter_enabled(
                analyse_type=TICKET_ANALYSE_TYPE_STATIC,
                db_type=DB_ORACLE
            )
        if kwargs.get("dynamic_rules_qs", None) is None:
            kwargs["dynamic_rules_qs"] = TicketRule.filter_enabled(
                type=TICKET_ANALYSE_TYPE_DYNAMIC,
                db_type=DB_ORACLE
            )
        super(OracleSubTicketAnalysis, self).__init__(**kwargs)
        self.schema_name = self.ticket.schema_name \
            if self.ticket.schema_name else self.cmdb.user_name
        self.cmdb_connector = OracleCMDBConnector(self.cmdb)
        self.cmdb_connector.execute(f"alter session set current_schema={self.schema_name}")

    def write_sql_plan(self, list_of_plan_dicts, **kwargs) -> mongoengine_qs:
        """写入执行计划"""
        OracleTicketSQLPlan.add_from_dict(
            self.ticket.work_list_id,
            self.cmdb.cmdb_id,
            self.schema_name,
            list_of_plan_dicts
        )
        return OracleTicketSQLPlan.objects(statement_id=kwargs["statement_id"])

    def run_dynamic(self,
                    sub_result: OracleTicketSubResult,
                    single_sql: dict):
        """动态分析"""
        try:
            statement_id = uuid.uuid1().hex[30:]  # oracle的statement_id字段最长30位
            formatted_sql = self.sql_filter_annotation(single_sql["sql_text"])
            self.cmdb_connector.execute("EXPLAIN PLAN SET "
                                        f"statement_id='{statement_id}' for {formatted_sql}")
            sql_for_getting_plan = f"SELECT * FROM plan_table " \
                                   f"WHERE statement_id = '{statement_id}'"
            sql_plans = self.cmdb_connector.select_dict(sql_for_getting_plan, one=False)
            if not sql_plans:
                raise Exception(f"fatal: No plans for statement_id: {statement_id}, "
                                f"sql: {formatted_sql}")
            sql_plan_qs = self.write_sql_plan(sql_plans, statement_id=statement_id)
            for dr in self.dynamic_rules:
                sub_result_item = TicketSubResultItem()
                sub_result_item.as_sub_result_of(dr)

                # ===这里指明了动态审核的输入参数(kwargs)===
                ret = dr.analyse(
                    single_sql=single_sql,
                    cmdb_connector=self.cmdb_connector,
                    mongo_connector=self.mongo_connector,
                    sql_plan_qs=sql_plan_qs,
                    schema_name=sub_result.schema_name
                )
                for output, current_ret in zip(dr.output_params, ret[1]):
                    sub_result_item.add_output(**{
                        **output,
                        "value": current_ret
                    })
                sub_result_item.weight = ret[0]
                sub_result.static.append(sub_result_item)
        except Exception as e:
            sub_result.error_msg = e
            trace = traceback.format_exc()
            print(e)
            print(trace)

    def run(self,
            sqls: [dict],
            single_sql: dict,
            **kwargs) -> OracleTicketSubResult:
        """
        单条sql的静态动态审核
        :param single_sql: {"sql_text":,"comments":,"num":,"sql_type"}
        :param sqls: [{single_sql},...]
        :param kwargs:
        """
        single_sql_text = single_sql["sql_text"]
        sub_result = OracleTicketSubResult(
            work_list_id=self.ticket.work_list_id,
            cmdb_id=self.cmdb.cmdb_id,
            schema_name=self.schema_name,
            position=single_sql["num"],
            sql_text=single_sql_text,
            comments=single_sql["comments"]
        )
        self.run_static(sub_result, sqls, single_sql)
        if single_sql["sql_type"] == SQL_DML:
            # TODO 确认一下是否只有DML需要执行计划
            self.run_dynamic(sub_result, single_sql)
        return sub_result
