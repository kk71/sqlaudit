# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "TicketScript",
    "Ticket",
    "TempScriptStatement"
]

import uuid

from mongoengine import IntField, StringField, DateTimeField, FloatField, \
    DynamicEmbeddedDocument, EmbeddedDocumentListField, EmbeddedDocumentField

import utils.const
from .parsed_sql import ParsedSQL
from core.ticket import *
from new_models.mongoengine import *
from utils import datetime_utils
from . import const


class TicketScript(
        DynamicEmbeddedDocument,
        BaseTicketScript,
        metaclass=ABCDocumentMetaclass):
    """工单脚本"""

    script_id = StringField(required=True, default=lambda: uuid.uuid4().hex)
    script_name = StringField()
    db_type = StringField(choices=utils.const.ALL_SUPPORTED_DB_TYPE)
    sub_ticket_count = IntField(default=0)


class Ticket(BaseDoc, BaseTicket, metaclass=ABCTopLevelDocumentMetaclass):
    """工单"""

    ticket_id = StringField(primary_key=True)
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

    statement_id = StringField(primary_key=True)
    script = EmbeddedDocumentField(TicketScript)
    position = IntField()  # 语句所在位置
    comment = StringField(default="")

    # 以下字段是ParsedSQLStatement封装的
    normalized = StringField()  # 处理后的单条sql语句
    normalized_without_comment = StringField()  # 处理后的单条无注释sql语句
    statement_type = StringField()  # 语句归属（select update delete etc...）
    sql_type = StringField(choices=const.ALL_SQL_TYPE)  # sql type: ddl or dml or ...

    meta = {
        "collection": "ticket_temp_parsed_sql_statement",
        "allow_inheritance": True,
        'indexes': [
            "script.script_id",
            "position",
            "sql_type"
        ]
    }

    @classmethod
    def parse_script(
            cls,
            script_text,
            script: TicketScript,
            filter_sql_type: str = None) -> ["TempScriptStatement"]:
        """
        处理一个sql脚本
        :param script_text:
        :param script:
        :param filter_sql_type: 是否过滤sql_type
        :return:
        """
        return [
            cls(
                script=script,
                position=i,
                ** parsed_sql_statement.serialize()
            )
            for i, parsed_sql_statement in enumerate(ParsedSQL(script_text))
            if filter_sql_type is None or parsed_sql_statement.sql_type == filter_sql_type
        ]

    def gen_tokens(self):
        return ParsedSQL(self.normalized)[0].tokens

    def parse_single_statement(self, sql_text: str):
        """
        处理单个sql语句
        :param sql_text:
        :return:
        """
        new_temp_statements = self.parse_script(sql_text, script=None)
        assert len(new_temp_statements) == 1
        self.from_dict(new_temp_statements[0].to_dict(
            iter_if=lambda k, v: k in (
                "normalized",
                "normalized_without_comment",
                "statement_type",
                "sql_type"
            )
        ))
