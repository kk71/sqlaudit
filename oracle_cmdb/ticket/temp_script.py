# Author: kk.Fang(fkfkbill@gmail.com)

from mongoengine import IntField, StringField, EmbeddedDocumentField

import utils.const
from new_models.mongoengine import BaseDoc
from ticket.ticket import TicketScript, const
from ticket.parsed_sql import ParsedSQLStatement


class TempParsedSQLStatement(BaseDoc, ParsedSQLStatement):
    """编辑临时脚本单条sql的暂存集合"""

    db_type = StringField(choices=utils.const.ALL_SUPPORTED_DB_TYPE)
    script = EmbeddedDocumentField(TicketScript)
    position = IntField()  # 语句所在位置
    comment = StringField(default="")

    # 以下字段是ParsedSQLStatement封装的
    normalized = StringField()  # 处理后的单条sql语句
    normalized_without_comment = StringField()  # 处理后的单条无注释sql语句
    tokens = StringField()  # sql语句内的结构(pickle)
    statement_type = StringField()  # 语句归属（select update delete etc...）
    sql_type = StringField(choices=const.ALL_SQL_TYPE)  # sql type: ddl or dml or ...

    meta = {
        "collection": "ticket_temp_parsed_sql_statement",
        'indexes': [
            "db_type",
            "position",
            "sql_type"
        ]
    }

    @classmethod
    def parse_script(cls, script_text, script: TicketScript):
        """处理一个sql脚本"""
