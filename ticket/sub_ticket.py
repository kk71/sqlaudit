# Author: kk.Fang(fkfkbill@gmail.com)

from mongoengine import IntField, StringField, DateTimeField, FloatField, \
    BooleanField, EmbeddedDocumentField, EmbeddedDocumentListField, ListField, \
    EmbeddedDocument

import utils.const
from . import const
from core.ticket import *
from new_models.mongoengine import *
from .ticket import TicketScript


class SubTicketIssue(EmbeddedDocument):
    """子工单的一个规则结果"""
    db_type = StringField(required=True)
    rule_name = StringField(required=True)
    rule_desc = StringField(required=True)
    max_score = FloatField(required=True)  # 最大扣分快照
    input_params = ListField(default=lambda: [])  # 输入参数快照
    output_params = ListField(default=lambda: [])  # 运行输出
    minus_score = FloatField(default=0)  # 当前规则的扣分，负数

    def get_rule_unique_key(self) -> tuple:
        return self.db_type, self.rule_name

    def as_sub_result_of(self, rule_object):
        """
        作为一个子工单（一条sql语句）的一个规则的诊断结果，获取该规则的信息
        :param rule_object:
        :return:
        """
        self.db_type = rule_object.db_type
        self.rule_name = rule_object.name
        self.rule_desc = rule_object.desc
        self.max_score = rule_object.max_score
        self.input_params = [i for i in rule_object.to_dict()["input_params"]]

    def add_output(self, output_structure, value):
        """
        :param output_structure: new_rule.rule.RuleInputOutputParams
        :param value:
        :return:
        """
        to_add = dict(
            name=output_structure.name,
            desc=output_structure.desc,
            unit=output_structure.unit,
            value=value
        )
        self.output_params.append(to_add)


class SubTicket(BaseDoc, BaseSubTicket, metaclass=ABCTopLevelDocumentMetaclass):
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