# Author: kk.Fang(fkfkbill@gmail.com)

from typing import Union
from copy import deepcopy

from mongoengine import IntField, StringField, DateTimeField, FloatField, \
    BooleanField, EmbeddedDocument, EmbeddedDocumentListField, DynamicField

from .utils import BaseDoc
from utils import const
from utils.datetime_utils import *
from utils.parsed_sql import *


class TicketOutputParams(EmbeddedDocument):
    """输出参数"""
    name = StringField(required=True)
    desc = StringField()
    unit = StringField()
    value = DynamicField()


class TicketSubResultItem(EmbeddedDocument):
    """子工单的一个规则结果"""
    db_type = StringField(required=True)
    rule_name = StringField(required=True)
    rule_desc = StringField(required=True)
    input_params = EmbeddedDocumentListField(
        TicketOutputParams)  # 记录规则执行时的输入参数快照
    output_params = EmbeddedDocumentListField(TicketOutputParams)  # 运行输出
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
        self.input_params = deepcopy(rule_object.input_params)

    def add_output(self, output_structure: TicketOutputParams, value):
        to_add = TicketOutputParams(
            name=output_structure.name,
            desc=output_structure.desc,
            unit=output_structure.unit,
            value=value
        )
        self.output_params.append(to_add)


class TicketSubResult(BaseDoc):
    """
    子工单
    请注意：这个子工单类是需要被不同的子类继承的，以兼顾oracle和mysql，
    在子工单列表里读取的时候使用本类进行操作以达到兼容效果，写入数据的时候请使用子类
    """
    work_list_id = IntField(required=True)
    task_name = StringField(default=None)
    cmdb_id = IntField()
    db_type = StringField()
    statement_id = StringField()  # sql_id
    sql_type = IntField(choices=const.ALL_SQL_TYPE)
    sql_text = StringField()
    sql_text_no_comment = StringField()
    comments = StringField(default="")
    position = IntField()  # 该语句在整个工单里的位置，从0开始
    static = EmbeddedDocumentListField(TicketSubResultItem)
    dynamic = EmbeddedDocumentListField(TicketSubResultItem)
    online_date = DateTimeField(default=None)  # 上线日期
    online_operator = StringField()  # 上线操作员
    online_status = BooleanField(default=None)  # 上线是否成功
    elapsed_seconds = IntField(default=None)  # 执行时长
    # 额外错误信息
    # 如果存在额外错误信息，则当前子工单未正确分析
    error_msg = StringField(null=True)
    check_time = DateTimeField(default=datetime.now)  # 分析日期

    meta = {
        "allow_inheritance": True,  # 子类继承本类，但是数据存在同一个collection里
        "collection": "ticket_sub_result",
        'indexes': [
            "db_type",
            "cmdb_id",
            "work_list_id",
            "statement_id",
            "position",
            "check_time",
            "online_status"
        ]
    }


class TicketSQLPlan(BaseDoc):
    """工单动态审核产生的执行计划，基类"""

    work_list_id = IntField(required=True)
    cmdb_id = IntField()
    etl_date = DateTimeField(default=lambda: arrow.now().datetime)

    meta = {
        'abstract': True,
        'indexes': [
            "work_list_id",
            "cmdb_id",
            "etl_date",
        ]
    }

    @classmethod
    def add_from_dict(cls,
                      work_list_id: int,
                      cmdb_id: int,
                      schema_name: str,
                      list_of_plan_dicts: list) -> list:
        """从字典增加执行计划"""
        raise NotImplementedError


class TicketMeta(BaseDoc):
    """工单的附加信息"""

    session_id = StringField(required=True)
    work_list_id = IntField()
    cmdb_id = IntField()
    original_sql = StringField(required=True)  # 原始上传的sql脚本文本
    comment_striped_sql = StringField(required=True)  # 去掉注释的sql

    meta = {
        "collection": "ticket_meta",
        'indexes': [
            "session_id",
            "work_list_id",
            "cmdb_id"
        ]
    }

    def generate_parsed_sql(self):
        return ParsedSQL(self.original_sql)
