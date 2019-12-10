# Author: kk.Fang(fkfkbill@gmail.com)

import traceback
from typing import Union, Callable
from copy import deepcopy

from mongoengine import IntField, StringField, DateTimeField, FloatField, \
    BooleanField, EmbeddedDocument, EmbeddedDocumentListField, \
    DynamicField, ListField

from .utils import BaseDoc
from utils import const
from utils.datetime_utils import *


class TicketRuleInputOutputParams(EmbeddedDocument):
    """输入输出参数"""
    name = StringField(required=True)
    desc = StringField()
    unit = StringField()
    # 此字段在ticket_rule的output_params里没有意义
    value = DynamicField(default=None, required=False, null=True)


class TicketRule(BaseDoc):
    """线下审核工单的规则"""
    name = StringField(required=True)
    desc = StringField(required=True)
    sql_type = StringField(choices=const.ALL_SQL_TYPE)  # 线下审核SQL的类型
    ddl_type = StringField(choices=const.ALL_DDL_TYPE)  # 线下审核DDL的详细分类(暂时没什么用)
    db_type = StringField(
        required=True,
        choices=const.ALL_SUPPORTED_DB_TYPE,
        default=const.DB_ORACLE)
    input_params = EmbeddedDocumentListField(TicketRuleInputOutputParams)
    output_params = EmbeddedDocumentListField(TicketRuleInputOutputParams)
    max_score = IntField()
    code = StringField(required=True)  # 规则的python代码
    status = BooleanField(default=True)  # 规则是否启用
    summary = StringField()  # 比desc更详细的一个规则解说
    type = StringField(required=True)
    solution = ListField()
    weight = FloatField(required=True)

    meta = {
        "collection": "ticket_rule",
        'indexes': [
            {'fields': ("db_type", "name"), 'unique': True}
        ]
    }

    def __init__(self, *args, **kwargs):
        super(TicketRule, self).__init__(*args, **kwargs)
        self._code: Union[Callable, None] = None

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

    def unique_key(self) -> tuple:
        return self.db_type, self.name

    def analyse(self, sql_text: str, cmdb_connector=None):
        """
        在给定的sql文本上执行当前规则
        :param sql_text: 单条待分析的sql语句
        :param cmdb_connector: 如果是静态规则，可能不需要当前纳管库的连接。这个完全取决于规则代码。
        :return:
        """
        try:
            if getattr(self, "_code", None):
                code_func = self._code
            else:
                print("generating code function for ticket rule "
                      f"{self.unique_key()}...")
                exec(self.code)
                code_func = code  # 这个code是在代码里面的
                # 放进去可以在当前对象存活周期内，不用每次都重新生成新的代码
                self._code: Callable = code_func
            return code_func(sql_text, cmdb_connector)
        except Exception as e:
            # 执行规则代码失败，需要报错
            trace = traceback.format_exc()
            print("failed when executing(or generating) ticket rule "
                  f"{self.unique_key()}: {e}")
            print(trace)
            raise const.RuleCodeInvalidException(trace)

    @classmethod
    def filter_enabled(cls):
        """仅过滤出开启的规则"""
        return cls.objects.filter(status=True)


class TicketSubResultItem(EmbeddedDocument):
    """子工单的一个规则的诊断"""
    db_type = StringField(required=True)
    rule_name = StringField(required=True)
    input_params = EmbeddedDocumentListField(
        TicketRuleInputOutputParams)  # 记录规则执行时的输入参数快照
    output_params = EmbeddedDocumentListField(TicketRuleInputOutputParams)  # 运行输出
    score_to_minus = FloatField(default=0)

    def get_rule_unique_key(self) -> tuple:
        return self.db_type, self.rule_name

    def as_sub_result_of(self, rule_object: TicketRule):
        """
        作为一个子工单（一条sql语句）的一个规则的诊断结果，获取该规则的信息
        :param rule_object:
        :return:
        """
        self.db_type = rule_object.db_type
        self.rule_name = rule_object.name
        self.input_params = deepcopy(rule_object.input_params)

    def add_output(self, **kwargs):
        self.output_params.append(TicketRuleInputOutputParams(**kwargs))


class TicketSubResult(BaseDoc):
    """子工单"""
    work_list_id = IntField(required=True)
    cmdb_id = IntField()
    schema_name = StringField()
    statement_id = StringField()  # sql_id
    sql_type = IntField(choices=const.ALL_SQL_TYPE)
    sql_text = StringField()
    comments = StringField()
    position = IntField()  # 该语句在整个工单里的位置，从0开始
    static = EmbeddedDocumentListField(TicketSubResultItem)
    dynamic = EmbeddedDocumentListField(TicketSubResultItem)
    online_status = BooleanField()  # 上线是否成功
    elapsed_seconds = IntField()  # 执行时长
    error_msg = StringField(null=True)  # 额外错误信息
    check_time = DateTimeField(default=datetime.now)

    meta = {
        "collection": "ticket_sub_result",
        'indexes': [
            "work_list_id",
            "cmdb_id",
            "statement_id",
            "position",
            "check_time",
        ]
    }

