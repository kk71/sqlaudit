# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleSubTicketAnalyse",
]

import uuid
import base64
import traceback

from cx_Oracle import DatabaseError

import ticket.const
import ticket.exceptions
import cmdb.const
import ticket.sub_ticket
from models.mongoengine import *
from plain_db.oracleob import *
from utils.parsed_sql import ParsedSQL
from ticket.analyse import SubTicketAnalyse
from .sub_ticket import OracleSubTicket
from .sql_plan import OracleTicketSQLPlan


class OracleSubTicketAnalyse(SubTicketAnalyse):
    """oracle子工单分析"""

    db_type = cmdb.const.DB_ORACLE

    @staticmethod
    def sql_filter_annotation(sql):
        """用于去掉每句sql末尾的分号(目前仅oracle需要这么做)"""
        if not sql:
            return ""
        sql = sql[:-1] if sql and sql[-1] == ";" else sql
        return sql.strip()

    def __init__(self, **kwargs):
        super(OracleSubTicketAnalyse, self).__init__(**kwargs)
        self.schema_name = self.ticket.schema_name \
            if self.ticket.schema_name else self.cmdb.user_name
        self.cmdb_connector = OracleCMDBConnector(self.cmdb)
        self.cmdb_connector.execute(f"alter session set current_schema={self.schema_name}")
        self.statement_id = base64.b64encode(uuid.uuid4().bytes).decode("utf-8")

    def write_sql_plan(self, list_of_plan_dicts, **kwargs) -> mongoengine_qs:
        """写入执行计划"""
        OracleTicketSQLPlan.add_from_dict(
            self.statement_id,
            self.ticket.ticket_id,
            self.cmdb.cmdb_id,
            schema_name=self.schema_name,
            list_of_plan_dicts=list_of_plan_dicts
        )
        return OracleTicketSQLPlan.objects(statement_id=self.statement_id)

    def run_dynamic(self,
                    sub_result: OracleSubTicket,
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
                if single_sql["sql_type"] not in dr.entries:
                    # 这里默认sql type和规则entries的类型在文本层面是相等的
                    # 实际都是文本，注意发生更改需要修改
                    continue
                # ===指明oracle动态审核的输入参数(kwargs)===
                ret = dr.run(
                    entries=self.dynamic_rules.entries,

                    single_sql=single_sql,
                    cmdb_connector=self.cmdb_connector,
                    mongo_connector=self.mongo_connector,
                    sql_plan_qs=sql_plan_qs,
                    schema_name=sub_result.schema_name,
                    statement_id=self.statement_id
                )
                for minus_score, output_param in ret:
                    sub_result_issue = ticket.sub_ticket.SubTicketIssue(
                        minus_score=minus_score)
                    sub_result_issue.as_issue_of(dr, output_data=output_param)
                    sub_result.dynamic.append(sub_result_issue)
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
            **kwargs) -> OracleSubTicket:
        """
        单条sql的静态动态审核
        :param single_sql:
        :param sqls: [{single_sql},...]
        :param kwargs:
        """
        _for_print = {
            k: v.strip()[:20] + '...'
            if isinstance(v, str) and len(v.strip()) > 20 else v
            for k, v in single_sql.items()
        }
        print(f"* {_for_print} of {len(sqls)}")
        ps = ParsedSQL(single_sql["sql_text"])
        if len(ps) != 1:
            raise ticket.exceptions.TicketAnalyseException(
                f"sub ticket with more than one sql sentence: {ps}")
        sub_result = OracleSubTicket(
            statement_id=self.statement_id,
            ticket_id=str(self.ticket.ticket_id),
            task_name=self.ticket.task_name,
            cmdb_id=self.cmdb.cmdb_id,
            db_type=cmdb.const.DB_ORACLE,
            schema_name=self.schema_name,
            **single_sql
        )
        self.run_static(sub_result, sqls, single_sql)
        if ps[0].statement_type not in ticket.const.SQL_KEYWORDS_NO_DYNAMIC_ANALYSE:
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
