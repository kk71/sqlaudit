# Author: kk.Fang(fkfkbill@gmail.com)

import traceback
from typing import Union, Callable, Optional
from copy import deepcopy

from mongoengine import IntField, StringField, DateTimeField, FloatField, \
    BooleanField, EmbeddedDocument, EmbeddedDocumentListField, \
    DynamicField, ListField
from schema import Schema, Or, And

from utils.schema_utils import *
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
        null=True, choices=const.ALL_TICKET_ANALYSE_TYPE)  # 规则类型，静态还是动态
    sql_type = IntField(
        null=True,
        choices=const.ALL_SQL_TYPE)  # 线下审核SQL的类型,为None则表示规则不区分sql_type
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

    def __str__(self):
        return "TicketRule:" + "-".join([str(i) for i in self.unique_key()
                                         if i is not None])

    @staticmethod
    def code_template():
        """
        返回code的模板
        :return:
        """
        return '''# code template for offline ticket rule
# 如果有任何import，请在此处导入，方便在规则执行前进行检查。


def code(rule, **kwargs):
    
    # kwargs存放当前规则类型下会给定的输入参数，
    # 可能包括纳管库连接对象，当前工单的全部语句，当前停留在的语句索引，执行计划等等。具体看业务。
    # 可通过self活得当前规则的参数信息，
    # 输入参数可使用rule.gip()获取单个输入参数的值

    minus_score = -rule.weight
                           # 扣分，-rule.weight即默认按照权重扣分
                           #      为0或者为None表示不扣分,
                           #      为负数则扣相应的分
                           #      正数报错
    output_params = []     # 按照输出的顺序给出返回的数据(list)
    return minus_score, output_params


code_hole.append(code)
        '''

    def unique_key(self) -> tuple:
        return self.db_type, self.name

    def gip(self, param_name: str) -> dict:
        """
        获取输入参数的值
        函数名gip是get input parameter的缩写
        :param param_name:
        :return:
        """
        return {i["name"]: i["value"]
                for i in self.to_dict()["input_params"]}[param_name]

    def _construct_code(self, code: str) -> Callable:
        """构建code函数"""
        code_hole = []
        exec(code, {
            "code_hole": code_hole,
        })
        if len(code_hole) != 1 or not callable(code_hole[0]):
            raise const.RuleCodeInvalidException("code not put in to the hole!")
        return code_hole.pop()

    def analyse(self, test_only=False, **kwargs) -> Optional[Union[list, tuple]]:
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
                self._construct_code(self.code)
            except Exception as e:
                trace = traceback.format_exc()
                print("failed when generating ticket rule "
                      f"{self.unique_key()}: {e}")
                print(trace)
                raise const.RuleCodeInvalidException(trace)
            return
        try:
            if not getattr(self, "_code", None):
                print("* generating code function for ticket rule "
                      f"{self.unique_key()}...")
                # 放进去可以在当前对象存活周期内，不用每次都重新生成新的代码
                self._code: Callable = self._construct_code(self.code)

            ret = self._code(self, **kwargs)

            # 校验函数返回的结构是否合乎预期
            Schema((
                Or(
                    And(scm_num, lambda x: x <= 0),
                    None
                ),
                Or([object], (object,))
            )).validate(ret)
            if len(ret[1]) != len(self.output_params):
                raise const.RuleCodeInvalidException(
                    f"The length of the iterable ticket rule returned({len(ret)}) "
                    f"is not equal with defined in rule({len(self.output_params)})")

            if ret[0] is None:
                ret[0] = 0
            return ret
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
    minus_score = FloatField(default=0)  # 当前规则的扣分，负数

    def get_rule_unique_key(self) -> tuple:
        return self.db_type, self.rule_name

    def get_rule(self) -> Union[TicketRule, None]:
        """获取当前的规则对象"""
        return TicketRule. \
            filter_enabled(db_type=self.db_type, name=self.rule_name).\
            first()

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
    statement_id = StringField()  # sql_id
    sql_type = IntField(choices=const.ALL_SQL_TYPE)
    sql_text = StringField()
    comments = StringField(default="")
    position = IntField()  # 该语句在整个工单里的位置，从0开始
    static = EmbeddedDocumentListField(TicketSubResultItem)
    dynamic = EmbeddedDocumentListField(TicketSubResultItem)
    online_status = BooleanField(default=None)  # 上线是否成功
    elapsed_seconds = IntField(default=None)  # 执行时长
    # 额外错误信息
    # 如果存在额外错误信息，则当前子工单未正确分析
    error_msg = StringField(null=True)
    check_time = DateTimeField(default=datetime.now)

    meta = {
        "allow_inheritance": True,
        "collection": "ticket_sub_result",
        'indexes': [
            "work_list_id",
            "statement_id",
            "position",
            "check_time",
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
