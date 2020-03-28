# Author: kk.Fang(fkfkbill@gmail.com)

from ticket.ticket import TempScriptStatement
from ticket.parsed_sql import ParsedSQLStatement


class SingleSQL(dict):
    """传给规则使用的单条sql语句的信息"""

    def __init__(self,
                 sql_text: str,
                 sql_text_no_comment: str,
                 comments: str,
                 position: int,
                 sql_type: str,
                 **kwargs):
        kwargs.update(
            sql_text=sql_text,
            sql_text_no_comment=sql_text_no_comment,
            comments=comments,
            position=position,
            sql_type=sql_type,
        )
        super(SingleSQL, self).__init__(**kwargs)

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
