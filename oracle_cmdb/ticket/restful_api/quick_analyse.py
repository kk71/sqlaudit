# Author: kk.Fang(fkfkbill@gmail.com)

import utils.const
import rule.const
from restful_api.modules import *
from utils.schema_utils import *
from auth.restful_api.base import AuthReq
from ..sub_ticket import OracleSubTicket
from ticket.analyse import SubTicketAnalyseStaticCMDBIndependent
from ..single_sql import SingleSQL
from ticket.parsed_sql import ParsedSQL
from rule.rule_jar import *


@as_view()
class QuickSQLAnalyse(AuthReq):

    def post(self):
        """快速单条sql分析（仅静态）"""
        params = self.get_json_args(Schema({
            "sql_text": scm_unempty_str
        }))
        sql_text = params.pop("sql_text")

        rule_jar = RuleJar.gen_jar_with_entries(
            rule.const.RULE_ENTRY_TICKET_STATIC_CMDB_INDEPENDENT,
            db_type=utils.const.DB_ORACLE
        )
        stasci = SubTicketAnalyseStaticCMDBIndependent(rule_jar)
        statements = ParsedSQL(sql_text)
        if not statements:
            return self.resp_bad_req(msg="未发现sql语句。")
        sqls = [
            SingleSQL.gen_from_parsed_sql_statement(a_statement)
            for a_statement in statements
        ]
        ret = []
        for single_sql in sqls:
            the_sub_ticket = OracleSubTicket(sql_text=single_sql["sql_text"])
            stasci.run_static(
                sub_result=the_sub_ticket,
                single_sql=single_sql,
                sqls=sqls
            )
            ret.append(the_sub_ticket.to_dict())
        self.resp(ret)
