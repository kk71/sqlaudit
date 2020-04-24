# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "SingleSQLForOnline"
]

import parsed_sql.const
from parsed_sql.single_sql import *
from parsed_sql.parsed_sql import *
from .capture import *


class SingleSQLForOnline(SingleSQL):

    @classmethod
    def gen_from_sql_text(cls, sql_text: SQLText):
        sqls: [ParsedSQLStatement] = ParsedSQL(sql_text.longer_sql_text)
        if len(sqls) != 1:
            # print(f"warning: {sql_text} can't be parsed correctly. "
            #       "using plain text instead.")
            return cls(
                sql_text=sql_text.longer_sql_text,
                sql_text_no_comment=sql_text.longer_sql_text,
                comments="",
                position=0,
                sql_type=parsed_sql.const.SQL_ANY
            )
        else:
            sql: ParsedSQLStatement = sqls[0]
            return cls(
                sql_text=sql.normalized,
                sql_text_no_comment=sql.normalized_without_comment,
                comments="",
                position=0,
                sql_type=sql.sql_type
            )

