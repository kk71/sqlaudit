# Author: kk.Fang(fkfkbill@gmail.com)

from utils.schema_utils import *
from restful_api.views.base import BaseReq
from ..analyse import OracleSubTicketAnalyse
from ..sub_ticket import OracleSubTicket
from ..ticket import OracleTicket
from ..single_sql import SingleSQL
from ticket.parsed_sql import ParsedSQL


class QuickSingleSQLAnalyse(BaseReq):

    def post(self):
        """快速单条sql分析（仅静态）"""
        params = self.get_json_args(Schema({
            "sql_text": scm_unempty_str
        }))
        sql_text = params.pop("sql_text")

        osta = OracleSubTicketAnalyse(cmdb=None, ticket=OracleTicket())
        the_sub_ticket = OracleSubTicket()
        statements = ParsedSQL(sql_text)
        if not statements:
            return self.resp_bad_req(msg="未发现sql语句。")
        single_sql = SingleSQL.gen_from_parsed_sql_statement(statements[0])
        osta.run_static(
            sub_result=the_sub_ticket, single_sql=single_sql, sqls=[single_sql])
        ret = the_sub_ticket.to_dict(iter_if=lambda k, v: k == "static")
        self.resp(ret)
