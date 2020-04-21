# Author: kk.Fang(fkfkbill@gmail.com)

from parsed_sql.single_sql import *
from .ticket import TempScriptStatement
from parsed_sql.parsed_sql import ParsedSQLStatement


class SingleSQLForTicket(SingleSQL):

    @classmethod
    def gen_from_temp_script(cls, ts: TempScriptStatement):
        return cls(
            sql_text=ts.normalized,
            sql_text_no_comment=ts.normalized_without_comment,
            comments=ts.comment,
            position=ts.position,
            sql_type=ts.sql_type
        )

    @classmethod
    def gen_from_parsed_sql_statement(cls, pss: ParsedSQLStatement):
        return cls(
            sql_text=pss.normalized,
            sql_text_no_comment=pss.normalized_without_comment,
            comments="",
            position=0,
            sql_type=pss.sql_type,
            tokens=pss.tokens
        )
