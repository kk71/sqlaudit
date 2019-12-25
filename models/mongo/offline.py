# Author: kk.Fang(fkfkbill@gmail.com)

import traceback
from typing import Union, Callable, Optional
from copy import deepcopy

from mongoengine import IntField, StringField, DateTimeField, FloatField, \
    BooleanField, EmbeddedDocument, EmbeddedDocumentListField, \
    DynamicField, ListField

from .utils import BaseDoc
from utils import const
from utils.datetime_utils import *
from utils.parsed_sql import *


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
    analysis_type = StringField(
        required=True, choices=const.ALL_TICKET_RULE_TYPE)  # 规则类型，静态还是动态
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
    solution = ListField()
    weight = FloatField(required=True)

    meta = {
        "collection": "ticket_rule",
        'indexes': [
            {'fields': ("db_type", "name"), 'unique': True},
            "name",
            "analysis_type",
            "sql_type",
            "db_type",
            "status"
        ],
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
        return '''# code template for offline ticket rule
        
def code(**kwargs):
    
    # kwargs存放当前规则类型下会给定的输入参数，
    # 可能包括纳管库连接对象，当前工单的全部语句，当前停留在的语句索引，执行计划等等。具体看业务。
    # 可通过self活得当前规则的参数信息，
    # 输入参数可使用self.get_input_params()获取简化的{name: value}字典

    minus_score = 0      # 扣分
    outout_params = []   # 按照输出的顺序给出返回的数据(list),或者给出{name: value}这样的结构(dict)
    return minus_score, output_params
        '''

    def unique_key(self) -> tuple:
        return self.db_type, self.name

    def get_input_params(self) -> dict:
        """获取简单的输入参数"""
        return {i["name"]: i["value"] for i in self.to_dict()["input_params"]}

    def analyse(self, test_only=False, **kwargs) -> Optional[list, tuple]:
        """
        在给定的sql文本上执行当前规则
        :param test_only: 仅测试生成code代码函数，并不执行。
        :param kwargs: 别的参数，根据业务不同传入不同的参数，具体看业务实现
        :return:
        """
        if test_only:
            # 仅生成code函数，并不缓存，也不执行。
            if getattr(self, "_code", None):
                delattr(self, "_code")
            print("generating code function for ticket rule "
                  f"{self.unique_key()}(for test only)...")
            try:
                exec(self.code)
            except Exception as e:
                trace = traceback.format_exc()
                print("failed when generating ticket rule "
                      f"{self.unique_key()}: {e}")
                print(trace)
                raise const.RuleCodeInvalidException(trace)
            return
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
            return code_func(**kwargs)
        except Exception as e:
            # 执行规则代码失败，需要报错
            trace = traceback.format_exc()
            print("failed when executing(or generating) ticket rule "
                  f"{self.unique_key()}: {e}")
            print(trace)
            raise const.RuleCodeInvalidException(trace)

    @classmethod
    def filter_enabled(cls, *args, **kwargs):
        """仅过滤出开启的规则"""
        return cls.objects.filter(status=True).filter(*args, **kwargs)


class TicketSubResultItem(EmbeddedDocument):
    """子工单的一个规则的诊断"""
    db_type = StringField(required=True)
    rule_name = StringField(required=True)
    input_params = EmbeddedDocumentListField(
        TicketRuleInputOutputParams)  # 记录规则执行时的输入参数快照
    output_params = EmbeddedDocumentListField(TicketRuleInputOutputParams)  # 运行输出
    weight = FloatField(default=0)

    def get_rule_unique_key(self) -> tuple:
        return self.db_type, self.rule_name

    def get_rule(self) -> Union[TicketRule, None]:
        """获取当前的规则对象"""
        return TicketRule. \
            filter_enabled(db_type=self.db_type, name=self.rule_name).first()

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

    def calc_score(self, rule: Optional[TicketRule] = None):
        """
        计算这个当前子工单当前规则的分数
        :param rule: 如果传一个rule对象进来，则优先用这个对象去计算
                     不传也可，但是不传会手动查询新的规则对象，如果该对象已经禁用，则扣分计0
        """
        if not rule:
            rule = self.get_rule()
        if not rule:
            self.weight = 0
        self.weight = rule.weight


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


class TicketSQLPlan(BaseDoc):
    """工单动态审核产生的执行计划，基类"""

    work_list_id = IntField(required=True)
    cmdb_id = IntField()
    schema_name = StringField()
    create_date = DateTimeField(default=lambda: arrow.now().datetime)

    meta = {
        'abstract': True,
        'indexes': [
            "work_list_id",
            "cmdb_id",
            "schema_name",
            "create_date",
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
    schema_name = StringField()
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
