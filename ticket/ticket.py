# Author: kk.Fang(fkfkbill@gmail.com)

import uuid

from mongoengine import IntField, StringField, DateTimeField, FloatField, \
    EmbeddedDocument, EmbeddedDocumentListField, EmbeddedDocumentField

import utils.const
from .parsed_sql import ParsedSQL
from core.ticket import *
from new_models.mongoengine import *
from utils import datetime_utils
from . import const


class TicketScript(EmbeddedDocument, BaseTicketScript, metaclass=ABCDocumentMetaclass):
    """工单脚本"""

    script_id = StringField(required=True, default=lambda: uuid.uuid4().hex)
    script_name = StringField()
    db_type = StringField(choices=utils.const.ALL_SUPPORTED_DB_TYPE)
    dir_path = StringField()
    sub_ticket_count = IntField(default=0)


class Ticket(BaseDoc, BaseTicket, metaclass=ABCTopLevelDocumentMetaclass):
    """工单"""

    task_name = StringField(required=True)
    db_type = StringField()
    cmdb_id = IntField()
    sub_ticket_count = IntField(default=0)
    scripts = EmbeddedDocumentListField(TicketScript)
    submit_time = DateTimeField(default=lambda: datetime_utils.datetime.now())
    submit_owner = StringField()
    status = IntField(choices=const.ALL_TICKET_STATUS)
    audit_role_id = IntField()
    audit_owner = StringField()
    audit_time = DateTimeField()
    audit_comment = StringField(default="")
    online_time = DateTimeField()
    score = FloatField()

    meta = {
        "allow_inheritance": True,
        "collection": "ticket",
        'indexes': [
            "task_name",
            "db_type",
            "cmdb_id",
            "submit_time",
            "submit_owner",
            "status",
            "audit_role_id"
        ]
    }

    def calculate_score(self, *args, **kwargs) -> float:
        """
        计算工单当前分数
        :param args:
        :param kwargs:
        :return:
        """
        return


class TempScriptStatement(BaseDoc):
    """临时脚本语句"""

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
        "allow_inheritance": True,
        'indexes': [
            "db_type",
            "script.script_id",
            "position",
            "sql_type"
        ]
    }

    @classmethod
    def parse_script(
            cls, script_text, script: TicketScript) -> ["TempScriptStatement"]:
        """处理一个sql脚本"""
        return [
            cls(
                script=script,
                position=i,
                ** parsed_sql_statement.serialize()
            )
            for i, parsed_sql_statement in enumerate(ParsedSQL(script_text))
        ]
