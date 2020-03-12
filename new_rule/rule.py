# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "TicketRule",
    "RuleInputOutputParams"
]

import traceback
from typing import Union, Callable, Optional

from mongoengine import EmbeddedDocument, StringField, DynamicField,\
    IntField, EmbeddedDocumentListField, BooleanField, ListField, FloatField
from schema import Schema, Or, And

from models.mongo import BaseDoc
from utils import const
from utils.schema_utils import scm_num


class RuleInputOutputParams(EmbeddedDocument):
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
    analyse_type = StringField(
        null=True, choices=const.ALL_TICKET_ANALYSE_TYPE)  # 规则类型，静态还是动态
    sql_type = IntField(
        null=True,
        choices=const.ALL_SQL_TYPE)  # 线下审核SQL的类型,为None则表示规则不区分sql_type
    ddl_type = StringField(choices=const.ALL_DDL_TYPE)  # 线下审核DDL的详细分类(暂时没什么用)
    db_type = StringField(
        required=True,
        choices=const.ALL_SUPPORTED_DB_TYPE,
        default=const.DB_ORACLE)
    input_params = EmbeddedDocumentListField(RuleInputOutputParams)
    output_params = EmbeddedDocumentListField(RuleInputOutputParams)
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
            "analyse_type",
            "sql_type",
            "db_type",
            "status"
        ],
    }

    def __init__(self, *args, **kwargs):
        super(TicketRule, self).__init__(*args, **kwargs)
        self._code: Union[Callable, None] = None

    def __str__(self):
        return "Rule:" + "-".join(
            [self.analyse_type] +
            [str(i) for i in self.unique_key() if i is not None]
        )

    @classmethod
    def calc_score_max_sum(cls, *args, **kwargs):
        """
        计算某个类型的数据库的规则总最大分
        """
        return sum(cls.filter_enabled(*args, **kwargs).values_list("max_score"))

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

    @staticmethod
    def _construct_code(code: str) -> Callable:
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
            print(f"* generating code for {str(self)} (test only)...")
            try:
                self._construct_code(self.code)
            except Exception as e:
                trace = traceback.format_exc()
                print(f"* failed when generating {self.unique_key()}: {e}")
                print(trace)
                raise const.RuleCodeInvalidException(trace)
            return
        try:
            if not getattr(self, "_code", None):
                print(f"* generating and analysing code of {str(self)} ...")
                # 放进去可以在当前对象存活周期内，不用每次都重新生成新的代码
                self._code: Callable = self._construct_code(self.code)
            else:
                print(f"* analysing {str(self)} ...")
            ret = self._code(self, **kwargs)

            # 校验函数返回的结构是否合乎预期
            Schema((
                Or(
                    And(scm_num, lambda x: x <= 0),
                    lambda x: x is None
                ),
                Or([object], (object,))
            )).validate(ret)
            if ret[0] and len(ret[1]) != len(self.output_params):
                raise const.RuleCodeInvalidException(
                    f"The length of the iterable ticket rule returned({len(ret[1])}) "
                    f"is not equal with defined in rule({len(self.output_params)})")
            ret = list(ret)
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
