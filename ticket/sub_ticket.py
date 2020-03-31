# Author: kk.Fang(fkfkbill@gmail.com)

from mongoengine import IntField, StringField, DateTimeField, FloatField, \
    BooleanField, EmbeddedDocumentField, EmbeddedDocumentListField, \
    EmbeddedDocument, DictField

import new_rule.exceptions
from . import const
from core.ticket import *
from core.issue import *
from new_models.mongoengine import *
from .ticket import TicketScript
from new_rule.rule import TicketRule


class SubTicketIssue(
        EmbeddedDocument,
        BaseIssue,
        metaclass=ABCDocumentMetaclass):
    """子工单的一个规则结果"""
    db_type = StringField(required=True)
    rule_name = StringField(required=True)
    rule_desc = StringField(required=True)
    max_score = FloatField(required=True)  # 最大扣分快照
    level = IntField()  # 规则优先级
    input_params = DictField(default=lambda: {})  # 输入参数快照
    output_params = DictField(default=lambda: {})  # 运行输出
    minus_score = FloatField(default=0)  # 当前规则的扣分，负数

    def get_rule_unique_key(self) -> tuple:
        return self.db_type, self.rule_name

    def as_issue_of(self, rule: TicketRule, output_data: dict):
        """
        作为一个子工单（一条sql语句）的一个规则的诊断结果，获取该规则的信息
        :param rule:
        :param output_data:
        :return:
        """
        self.db_type = rule.db_type
        self.rule_name = rule.name
        self.rule_desc = rule.desc
        self.level = rule.level
        self.max_score = rule.max_score
        self.input_params = [i for i in rule.to_dict()["input_params"]]
        for output_param in rule.output_params:
            the_output_data_to_this_param = output_data.get(output_param.name, None)
            if not output_param.validate_data_type(the_output_data_to_this_param):
                raise new_rule.exceptions.RuleCodeInvalidParamTypeException
            self.output_params[output_param.name] = the_output_data_to_this_param


class SubTicket(
        BaseDoc,
        BaseSubTicket,
        metaclass=ABCTopLevelDocumentMetaclass):
    """
    子工单
    请注意：这个子工单类是需要被不同的子类继承的，以兼顾oracle和mysql，
    在子工单列表里读取的时候使用本类进行操作以达到兼容效果，写入数据的时候请使用子类
    """
    statement_id = StringField(primary_key=True)
    ticket_id = StringField(required=True)
    script = EmbeddedDocumentField(TicketScript)
    task_name = StringField(default=None)
    db_type = StringField()
    cmdb_id = IntField()
    sql_type = StringField(choices=const.ALL_SQL_TYPE)
    sql_text = StringField()
    sql_text_no_comment = StringField()
    comments = StringField(default="")
    position = IntField()
    static = EmbeddedDocumentListField(SubTicketIssue)
    dynamic = EmbeddedDocumentListField(SubTicketIssue)
    online_date = DateTimeField(default=None)  # 上线日期
    online_operator = StringField()  # 上线操作员
    online_status = BooleanField(default=None)  # 上线是否成功
    elapsed_seconds = IntField(default=None)  # 执行时长
    # 额外错误信息
    # 如果存在额外错误信息，则当前子工单未正确分析
    error_msg = StringField(null=True)

    meta = {
        "allow_inheritance": True,
        "collection": "sub_ticket",
        'indexes': [
            "statement_id",
            "ticket_id",
            "script.script_id",
            "task_name",
            "db_type",
            "cmdb_id",
            "position",
            "online_status"
        ]
    }

    def __repr__(self):
        return f"<SubTicket {self.statement_id}>"
