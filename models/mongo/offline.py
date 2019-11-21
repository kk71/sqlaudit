# Author: kk.Fang(fkfkbill@gmail.com)

import traceback
from typing import Union

from mongoengine import IntField, StringField, DateTimeField, FloatField, \
    BooleanField, EmbeddedDocument, EmbeddedDocumentListField, \
    DynamicField, ListField

from .utils import BaseDoc
from utils import const
from utils.datetime_utils import *


class TicketRuleInputOutputParams(EmbeddedDocument):
    """输入输出参数"""
    name = StringField()
    desc = StringField()
    unit = StringField()
    # 此字段在ticket_rule的output_params里没有意义
    value = DynamicField(required=False, null=True)


class TicketRule(BaseDoc):
    """线下审核工单的规则"""
    name = StringField(required=True)
    desc = StringField(required=True)
    sql_type = StringField(choices=const.ALL_SQL_TYPE)  # 线下审核SQL的类型
    ddl_type = StringField(choices=const.ALL_DDL_TYPE)  # 线下审核DDL的详细分类
    db_type = StringField(
        required=True,
        choices=const.ALL_SUPPORTED_DB_TYPE,
        default=const.DB_ORACLE)
    db_model = StringField(
        required=True, choices=const.ALL_SUPPORTED_DB_TYPE)
    input_params = EmbeddedDocumentListField(TicketRuleInputOutputParams)
    output_params = EmbeddedDocumentListField(TicketRuleInputOutputParams)
    max_score = IntField()
    code = StringField(required=True)
    status = BooleanField(default=True)
    summary = StringField()  # 比desc更详细的一个规则解说
    type = StringField(required=True)
    solution = ListField()
    weight = FloatField(required=True)

    meta = {
        "collection": "ticket_rule",
        'indexes': [
            {'fields': ("db_type", "db_model", "name"), 'unique': True}
        ]
    }

    @staticmethod
    def code_template():
        """
        返回code的模板
        :return:
        """
        return f'''# code template for offline ticket rule
        
def code(sql_text, cmdb_connector=None):
    return
        '''

    def get_3_key(self) -> tuple:
        return self.db_type, self.db_model, self.name

    def run(self, sql_text, cmdb_connector=None):
        """
        在给定的sql文本上执行当前规则
        :param sql_text:
        :param cmdb_connector:
        :return:
        """
        try:
            if getattr(self, "_code", None):
                code_func = self._code
            else:
                print("generating code function for ticket rule "
                      f"{self.get_3_key()}...")
                exec(self.code)
                code_func = code  # 这个code是在代码里面的
                # 放进去可以在当前对象存活周期内，不用每次都重新生成新的代码
                setattr(self, "_code", code_func)
            return code_func(sql_text, cmdb_connector)
        except Exception as e:
            # 执行规则代码失败，需要报错
            trace = traceback.format_exc()
            print("failed when executing(or generating) ticket rule "
                  f"{self.get_3_key()}: {e}")
            print(trace)
            raise const.RuleCodeInvalidException(trace)


class TicketResultSubResultItem(EmbeddedDocument):
    """一个子工单的诊断"""
    db_type = StringField(required=True)
    db_model = StringField(required=True)
    rule_name = StringField(required=True)
    input_params = EmbeddedDocumentListField(TicketRuleInputOutputParams)
    output_params = EmbeddedDocumentListField(TicketRuleInputOutputParams)
    score_to_minus = FloatField(default=0)


class TicketResultSubResult(EmbeddedDocument):
    """一个子工单的信息"""
    sub_work_list_id = IntField(required=True)
    work_list_id = IntField(required=True)
    sql_id = StringField()
    sql_text = StringField()
    static = EmbeddedDocumentListField(TicketResultSubResultItem)
    dynamic = EmbeddedDocumentListField(TicketResultSubResultItem)


class TicketResult(BaseDoc):
    """线下工单规则审核结果"""
    work_list_id = IntField()
    cmdb_id = IntField()
    schema_name = StringField()
    create_date = DateTimeField(default=datetime.now)
    results = EmbeddedDocumentListField(TicketResultSubResult)
    score = FloatField(default=0)

    meta = {
        "collection": "ticket_result",
        'indexes': [
            "work_list_id",
            "schema_name",
            "cmdb_id",
            "create_date"
        ]
    }
