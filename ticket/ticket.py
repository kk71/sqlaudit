# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "TicketScript",
    "Ticket",
    "TempScriptStatement"
]

import uuid
from typing import Union
from functools import reduce
from collections import defaultdict

from mongoengine import IntField, StringField, DateTimeField, FloatField, \
    DynamicEmbeddedDocument, EmbeddedDocumentListField, EmbeddedDocumentField, \
    ObjectIdField
from bson.objectid import ObjectId

import cmdb.const
import parsed_sql.const
from parsed_sql.parsed_sql import ParsedSQL
from core.ticket import *
from models.mongoengine import *
from utils import datetime_utils
from . import const


class TicketScript(
        DynamicEmbeddedDocument,
        BaseTicketScript,
        metaclass=ABCDocumentMetaclass):
    """工单脚本"""

    script_id = StringField(required=True, default=lambda: uuid.uuid4().hex)
    script_name = StringField()
    db_type = StringField(choices=cmdb.const.ALL_DB_TYPE)
    sub_ticket_count = IntField(default=0)

    def __repr__(self):
        return f"<TicketScript {self.script_id}-{self.script_name}>"

    def update_sub_ticket_count_from_sub_ticket(self):
        """从子工单处更新当前脚本的sub_ticket_count, TODO 需要手动保存工单"""
        from .sub_ticket import SubTicket
        self.sub_ticket_count = SubTicket.filter(
            script__script_id=self.script_id).count()


class Ticket(BaseDoc, BaseTicket, metaclass=ABCTopLevelDocumentMetaclass):
    """工单"""

    ticket_id = ObjectIdField(primary_key=True, default=ObjectId)
    db_type = StringField()
    task_name = StringField(required=True)
    cmdb_id = IntField()
    sub_ticket_count = IntField(default=0)
    scripts = EmbeddedDocumentListField(TicketScript)
    submit_time = DateTimeField(default=lambda: datetime_utils.datetime.now())
    submit_owner = StringField()
    status = IntField(default=const.TICKET_ANALYSING, choices=const.ALL_TICKET_STATUS)
    audit_role_id = IntField()
    audit_owner = StringField()
    audit_time = DateTimeField()
    audit_comments = StringField(default="")
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

    def __str__(self):
        return f"<Ticket {self.db_type}-{self.ticket_id}>"

    def calculate_score(
            self, at_least: Union[None, int, float] = 60, **kwargs) -> float:
        """
        计算工单当前分数
        :param at_least:
        :param kwargs:
        :return:
        """
        from rule.cmdb_rule import CMDBRule
        from .sub_ticket import SubTicket

        print(f"calculating total score for {self.ticket_id}...")
        # unique_key: (当前已扣, 最大扣分)
        rules_max_score = defaultdict(lambda: [0, 0])
        for sub_result in SubTicket.filter(ticket_id=str(self.ticket_id)):
            static_and_dynamic_results = sub_result.static + sub_result.dynamic
            for issue_of_sub_result in static_and_dynamic_results:
                rule_unique_key = issue_of_sub_result.get_rule_unique_key()
                rules_max_score[rule_unique_key][1] = issue_of_sub_result.max_score
                if rules_max_score[rule_unique_key][0] < \
                        rules_max_score[rule_unique_key][1]:
                    # 仅当已经扣掉的分数依然小于最大扣分的时候才继续扣分
                    rules_max_score[rule_unique_key][0] += \
                        issue_of_sub_result.minus_score  # 这个minus_score是负数或0！
                else:
                    # 否则，直接将扣分置为最大扣分
                    rules_max_score[rule_unique_key][0] = \
                        rules_max_score[rule_unique_key][1]
        if rules_max_score:
            total_minus_score, _ = reduce(
                lambda x, y: [x[0] + y[0], x[1] + y[1]],
                rules_max_score.values()
            )
        else:
            total_minus_score = 0
        all_rule_max_score_sum = CMDBRule.calc_score_max_sum(
            db_type=self.db_type,
            cmdb_id=self.cmdb_id
        )
        if all_rule_max_score_sum:
            final_score = (all_rule_max_score_sum + total_minus_score) / \
                          float(all_rule_max_score_sum) * 100.0
        else:
            final_score = 100
        if at_least and final_score < at_least:
            final_score = at_least
        self.score = round(final_score, 2)  # 未更新库中数据，需要手动加入session并commit
        return self.score

    def update_sub_ticket_count_from_scripts(self):
        """从当前工单的scripts.sub_ticket_count更新工单的全部子工单数，TODO 需要手动保存"""
        self.sub_ticket_count = sum([
            a_script.sub_ticket_count for a_script in self.scripts])


class TempScriptStatement(BaseDoc):
    """临时脚本语句"""

    statement_id = ObjectIdField(primary_key=True)
    script = EmbeddedDocumentField(TicketScript)
    position = IntField()  # 语句所在位置
    comment = StringField(default="")

    # 以下字段是ParsedSQLStatement封装的
    normalized = StringField()  # 处理后的单条sql语句
    normalized_without_comment = StringField()  # 处理后的单条无注释sql语句
    statement_type = StringField()  # 语句归属（select update delete etc...）
    sql_type = StringField(choices=parsed_sql.const.ALL_SQL_TYPE)

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
            if filter_sql_type is None or parsed_sql_statement.sql_type != filter_sql_type
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
