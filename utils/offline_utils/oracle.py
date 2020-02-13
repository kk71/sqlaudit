# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleSubTicketAnalysis",
]

import uuid
import base64
import traceback

from mongoengine import QuerySet as mongoengine_qs
from cx_Oracle import DatabaseError

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
                analyse_type=TICKET_ANALYSE_TYPE_DYNAMIC,
                db_type=DB_ORACLE
            )
        super(OracleSubTicketAnalysis, self).__init__(**kwargs)
        self.schema_name = self.ticket.schema_name \
            if self.ticket.schema_name else self.cmdb.user_name
        self.cmdb_connector = OracleCMDBConnector(self.cmdb)
        self.cmdb_connector.execute(f"alter session set current_schema={self.schema_name}")
        self.statement_id = base64.b64encode(uuid.uuid4().bytes).decode("utf-8")

    def write_sql_plan(self, list_of_plan_dicts, **kwargs) -> mongoengine_qs:
        """写入执行计划"""
        OracleTicketSQLPlan.add_from_dict(
            self.ticket.work_list_id,
            self.cmdb.cmdb_id,
            self.schema_name,
            list_of_plan_dicts
        )
        return OracleTicketSQLPlan.objects(statement_id=self.statement_id)

    def run_dynamic(self,
                    sub_result: OracleTicketSubResult,
                    single_sql: dict):
        """动态分析"""
        try:
            formatted_sql = self.sql_filter_annotation(single_sql["sql_text"])
            self.cmdb_connector.execute("EXPLAIN PLAN SET "
                                        f"statement_id='{self.statement_id}' "
                                        f"for {formatted_sql}")
            sql_for_getting_plan = f"SELECT * FROM plan_table " \
                                   f"WHERE statement_id = '{self.statement_id}'"
            sql_plans = self.cmdb_connector.select_dict(sql_for_getting_plan, one=False)
            if not sql_plans:
                raise Exception(f"fatal: No plans for statement_id: {self.statement_id}, "
                                f"sql: {formatted_sql}")
            sql_plan_qs = self.write_sql_plan(sql_plans)
            for dr in self.dynamic_rules:
                if dr.sql_type is not SQL_ANY and\
                        dr.sql_type != single_sql["sql_type"]:
                    continue
                sub_result_item = TicketSubResultItem()
                sub_result_item.as_sub_result_of(dr)

                # ===这里指明了动态审核的输入参数(kwargs)===
                score_to_minus, output_params = dr.analyse(
                    single_sql=single_sql,
                    cmdb_connector=self.cmdb_connector,
                    mongo_connector=self.mongo_connector,
                    sql_plan_qs=sql_plan_qs,
                    schema_name=sub_result.schema_name,
                    statement_id=self.statement_id
                )
                for output, current_ret in zip(dr.output_params, output_params):
                    sub_result_item.add_output(output, current_ret)
                sub_result_item.minus_score = score_to_minus
                if sub_result_item.minus_score != 0:
                    sub_result.dynamic.append(sub_result_item)
        except Exception as e:
            error_msg = str(e)
            if isinstance(e, DatabaseError):
                sub_result.error_msg = self.update_error_message(
                    "动态审核", msg=error_msg, trace="", old_msg=sub_result.error_msg)
            else:
                trace = traceback.format_exc()
                sub_result.error_msg = self.update_error_message(
                    "动态审核", msg=error_msg, trace=trace, old_msg=sub_result.error_msg)
                print(trace)
            print(error_msg)

    def run(self,
            sqls: [dict],
            single_sql: dict,
            **kwargs) -> OracleTicketSubResult:
        """
        单条sql的静态动态审核
        :param single_sql:
                {"sql_text":,"sql_text_no_comment":"comments":,"num":,"sql_type"}
        :param sqls: [{single_sql},...]
        :param kwargs:
        """
        _for_print = {
            k: v.strip()[:20] + '...'
            if isinstance(v, str) and len(v.strip()) > 20 else v
            for k, v in single_sql.items()
        }
        print(f"* {_for_print} of {len(sqls)}")
        sub_result = OracleTicketSubResult(
            work_list_id=self.ticket.work_list_id,
            cmdb_id=self.cmdb.cmdb_id,
            db_type=DB_ORACLE,
            schema_name=self.schema_name,
            statement_id=self.statement_id,
            position=single_sql.pop("num"),
            **single_sql
        )
        self.run_static(sub_result, sqls, single_sql)
        self.run_dynamic(sub_result, single_sql)
        return sub_result

    def sql_online(self, sql: str, **kwargs):
        """上线sql脚本"""
        username = kwargs["username"]
        password = kwargs["password"]
        odb = OracleCMDBConnector(
            self.cmdb, username=username, password=password)
        try:
            odb.execute(sql)
            odb.conn.commit()
            return ""
        except Exception as e:
            odb.conn.rollback()
            return str(e)
        finally:
            odb.conn.close()
