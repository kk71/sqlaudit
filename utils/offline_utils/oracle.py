# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleSubTicketAnalysis",
]

import uuid

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
                analyse_type=TICKET_RULE_STATIC,
                db_type=DB_ORACLE
            )
        if kwargs.get("dynamic_rules_qs", None) is None:
            kwargs["dynamic_rules_qs"] = TicketRule.filter_enabled(
                type=TICKET_RULE_DYNAMIC,
                db_type=DB_ORACLE
            )
        super(OracleSubTicketAnalysis, self).__init__(**kwargs)
        self.schema_name = self.ticket.schema_name \
            if self.ticket.schema_name else self.cmdb.user_name
        self.cmdb_connector = OracleCMDBConnector(self.cmdb)
        self.cmdb_connector.execute(f"alter session set current_schema={self.schema_name}")

    def write_sql_plan(self, **kwargs):
        """写入执行计划"""
        return

    def run_dynamic(self,
                    sub_result: OracleTicketSubResult,
                    single_sql: dict):
        """动态分析"""
        statement_id = uuid.uuid1().hex
        sub_result_item = TicketSubResultItem()
        formatted_sql = self.sql_filter_annotation(single_sql["sql_text"])
        self.cmdb_connector.execute("EXPLAIN PLAN SET "
                                    f"statement_id='{statement_id}' for {formatted_sql}")
        sql_for_getting_plan = f"SELECT * FROM plan_table " \
                               f"WHERE statement_id = '{statement_id}'"
        sql_plans = self.cmdb_connector.select_dict(sql_for_getting_plan, one=False)
        if not sql_plans:
            raise Exception(f"fatal: No plans for "
                            f"statement_id: {statement_id}, sql: {formatted_sql}")
        for dr in self.dynamic_rules:
            sub_result_item.as_sub_result_of(dr)
            OracleTicketSQLPlan.add_from_dict(
                self.ticket.work_list_id,
                self.cmdb.cmdb_id,
                self.schema_name,
                sql_plans
            )

    def run(self,
            sqls: [dict],
            single_sql: dict,
            **kwargs) -> OracleTicketSubResult:
        """
        单条sql的静态动态审核
        :param single_sql: {"sql_text":,"comments":,"num":}
        :param sqls: [{single_sql},...]
        :param kwargs:
        """
        single_sql_text = single_sql["sql_text"]
        sub_result = OracleTicketSubResult(
            cmdb_id=self.cmdb.cmdb_id,
            schema_name=self.schema_name,
            position=single_sql["num"],
            sql_text=single_sql_text,
            comments=single_sql["comments"]
        )
        self.run_static(sub_result, sqls, single_sql)
        self.run_dynamic(sub_result, single_sql)
        return sub_result
