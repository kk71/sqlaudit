# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "RuleInputParams",
    "RuleOutputParams",
    "BaseRule"
]

import traceback
from typing import Union, Callable, Dict, Any, List

from mongoengine import EmbeddedDocument, StringField, DynamicField, \
    IntField, EmbeddedDocumentListField, BooleanField, ListField, FloatField
from schema import Or, SchemaError, Use

import settings
import core.rule
import core.issue
import cmdb.const
from models.mongoengine import *
from utils.schema_utils import *
from . import exceptions
from . import const


class RuleParams(EmbeddedDocument):
    """规则的参数"""

    name = StringField(required=True)  # 唯一
    desc = StringField(default="")  # 描述
    unit = StringField(default="")  # 单位
    data_type = StringField(
        required=True, choices=const.ALL_RULE_PARAM_TYPES)  # 数据类型

    meta = {
        "allow_inheritance": True
    }

    def __str__(self):
        return f"<RuleParams {self.name}-{self.data_type}>"

    def validate_data_type(self, the_value):
        s = f"{self} in {self._instance} got {the_value=}, {type(the_value)=}"
        if self.data_type == const.RULE_PARAM_TYPE_STR:
            if not isinstance(the_value, str):
                raise exceptions.RuleCodeInvalidParamTypeException(s)
        elif self.data_type == const.RULE_PARAM_TYPE_INT:
            if not isinstance(the_value, int):
                raise exceptions.RuleCodeInvalidParamTypeException(s)
        elif self.data_type == const.RULE_PARAM_TYPE_FLOAT:
            if not isinstance(the_value, float):
                raise exceptions.RuleCodeInvalidParamTypeException(s)
        elif self.data_type == const.RULE_PARAM_TYPE_NUM:
            if not isinstance(the_value, (float, int)):
                raise exceptions.RuleCodeInvalidParamTypeException(s)
        elif self.data_type == const.RULE_PARAM_TYPE_LIST:
            if not isinstance(the_value, list):
                raise exceptions.RuleCodeInvalidParamTypeException(s)
        elif self.data_type == const.RULE_PARAM_TYPE_SET:
            if not isinstance(the_value, set):
                raise exceptions.RuleCodeInvalidParamTypeException(s)
        else:
            assert 0


class RuleInputParams(RuleParams):
    """输入参数"""

    value = DynamicField(default=None, required=True, null=True)

    def validate_input_data(self):
        """验证数据类型是否正确"""
        self.validate_data_type(self.value)


class RuleOutputParams(RuleParams):
    """输出参数"""

    # 标志此参数是否可以不返回
    # False 字段必须出现，数据类型必须符合预期，可为None
    # True 字段可选出现，数据类型不做检查，可为None
    optional = BooleanField(default=False)

    def validate_data_type(self, the_value):
        if the_value is None:
            return
        try:
            super().validate_data_type(the_value)
        except exceptions.RuleCodeInvalidParamTypeException as e:
            if not self.optional:
                raise e
            else:
                print(e)  # 如果当前输出可选，则只打印但不报错

    @classmethod
    def validate_output_data(
            cls,
            the_rule: "BaseRule",
            ret) -> [(Union[int, float], dict)]:
        """
        验证输出数据是否正确
        :param the_rule: 当前输出参数所在的规则。验证的是改规则的全部输出参数
        :param ret: 规则代码的直接返回结果
        :return:
        """
        try:
            # 校验函数返回的结构是否合乎预期
            ret = Schema(
                [
                    scm_optional(Or(
                        (
                            Or(
                                And(scm_num, lambda x: x <= 0),
                                Use(
                                    lambda x: -the_rule.weight
                                    if x is None
                                    else scm_raise_error(
                                        f"incorrect minus_score: {x}"
                                    )
                                )
                            ),
                            dict
                        ),
                        Use(
                            lambda x: (-the_rule.weight, x)
                            if isinstance(x, dict)
                            else scm_raise_error(f"no minus_score is given,"
                                                 f" and the return data is "
                                                 f"incorrect, too."))
                    ))
                ]
            ).validate(ret)
        except SchemaError:
            raise exceptions.RuleCodeInvalidReturnException(ret)
        output_params_keys = set()
        for the_output_param in the_rule.output_params:
            output_params_keys.add(the_output_param.name)
            for _, a_ret_dict in ret:
                if not the_output_param.optional and\
                            the_output_param.name not in a_ret_dict.keys():
                    # 逐输出字段并且逐输出数据检查必选的输出字段是否输出了
                    raise exceptions.RuleCodeInvalidReturnException(
                        f"{the_rule} requires output: {the_output_param.name}")
                if the_output_param.name in a_ret_dict.keys():
                    the_output_param.validate_data_type(
                        a_ret_dict[the_output_param.name])
        for _, a_ret_dict in ret:
            # 这个循环是为了去掉没有声明的输出字段
            # 规则代码中允许返回没有声明的字段，但是规则返回的时候这些字段需要去掉
            new_ret_dict = {
                k: v
                for k, v in a_ret_dict.items()
                if k in output_params_keys
            }
            a_ret_dict.clear()
            a_ret_dict.update(new_ret_dict)
        return ret


class BaseRule(
        BaseDoc,
        core.rule.BaseRuleItem,
        metaclass=ABCTopLevelDocumentMetaclass):
    """规则"""
    name = StringField(required=True)
    desc = StringField(required=True)
    db_type = StringField(
        required=True,
        choices=cmdb.const.ALL_DB_TYPE,
        default=cmdb.const.DB_ORACLE)
    entries = ListField(default=list, choices=const.ALL_RULE_ENTRIES)
    input_params = EmbeddedDocumentListField(RuleInputParams)
    output_params = EmbeddedDocumentListField(RuleOutputParams)
    code = StringField(required=True)
    status = BooleanField(default=True)
    summary = StringField()  # 比desc更详细的一个规则解说
    solution = ListField(default=list)
    weight = FloatField(required=True)
    max_score = IntField()
    level = IntField(
        default=const.RULE_LEVEL_WARNING,
        choices=const.ALL_RULE_LEVELS)  # 规则等级

    # 记录基础规则的全部字段名
    ALL_KEYS = (
        "name",
        "desc",
        "db_type",
        "entries",
        "input_params",
        "output_params",
        "code",
        "status",
        "summary",
        "solution",
        "weight",
        "max_score",
        "level"
    )

    meta = {
        "abstract": True,
        'indexes': [
            "name",
            "db_type",
            "entries",
            "status",
            "level"
        ],
    }

    def __init__(self, *args, **kwargs):
        super(BaseRule, self).__init__(*args, **kwargs)
        self._code: Union[Callable, None] = None

    def __str__(self):
        return f"<Rule " + "-".join(
            [str(i) for i in self.unique_key() if i is not None]
        ) + ">"

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
        return '''# rule code template
# 如果有任何import，请在此处导入，方便在规则执行前进行检查。


def code(rule, entries: [str], **kwargs):
    
    # entries 指明了本次调用是什么层面的调用。
    # kwargs存放当前规则类型下会给定的输入参数，
    # 可能包括纳管库连接对象，当前工单的全部语句，当前停留在的语句索引，执行计划等等。具体看业务。
    # 可通过rule获得当前规则的参数信息，
    # 输入参数可使用rule.gip()获取单个输入参数的值

    minus_score =               # 扣分，None或者-rule.weight即默认按照权重扣分
                                #      为0表示不扣分,
                                #      为负数则扣相应的分
                                #      正数报错
    output_params_dict =        # 输出参数的dict
    
    # 一次执行可以多次yield，无论扣分是否为0，只要出现一次yield，就触发一次规则
    
    yield minus_score, output_params_dict
    
    # 省略minus_score的形式也是可以的,默认使用-rule.weight
    yield output_params_dict


code_hole.append(code)  # 务必加上这一句
'''

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
    def construct_code(code: str) -> Callable:
        """构建code函数"""
        code_hole = []
        exec(code, {
            "code_hole": code_hole,
        })
        if len(code_hole) != 1:
            raise exceptions.RuleCodeInvalidException(
                "code should be put into the hole")
        if not callable(code_hole[0]):
            raise exceptions.RuleCodeInvalidException(
                "the object put into the hole is not callable")
        return code_hole.pop()

    def test(self):
        """仅生成code函数，不缓存。并且执行规则输入参数每个参数的验证。"""
        print(f"* validating input params of {str(self)}...")
        for input_param in self.input_params:
            input_param.validate_input_data()
        if getattr(self, "_code", None):
            delattr(self, "_code")
        print(f"* generating code of {str(self)} (test only)...")
        self.construct_code(self.code)

    def run(
            self,
            entries: [str] = None,
            **kwargs):
        """
        在给定的sql文本上执行当前规则
        :param entries: 入口，告诉规则函数，本次调用是从哪里调入的，不同调用入口，会带入不同的参数
        :param kwargs: 别的参数，根据业务不同传入不同的参数，具体看业务实现
        :return:
        """
        try:
            if not getattr(self, "_code", None):
                # 放进去可以在当前对象存活周期内，不用每次都重新生成新的代码
                self._code: Callable = self.construct_code(self.code)
            if settings.RULE_DEBUG:
                print(f"* running {str(self)}: {entries=} {kwargs=} ...")
            ret = list(self._code(self, entries, **kwargs))

        except Exception as e:
            # 执行规则代码失败，需要报错
            trace = traceback.format_exc()
            print(f"failed when executing(or generating) {str(self)}: {e}")
            print(trace)
            raise exceptions.RuleCodeInvalidException(trace)
        current_rule_output_params = self.__class__.\
            output_params.field.document_type_obj
        return current_rule_output_params.validate_output_data(self, ret)

    @classmethod
    def filter_enabled(cls, *args, **kwargs):
        """仅过滤出开启的规则"""
        return cls.filter(status=True).filter(*args, **kwargs)

    def to_dict(self, *args, need_unique_key=False, **kwargs) -> dict:
        origin_ret = super().to_dict(*args, **kwargs)
        if need_unique_key:
            # 为输出的字典增加规则唯一键
            for k in self.UNIQUE_KEYS:
                origin_ret[k] = getattr(self, k)
        return origin_ret

    def format_output_params(self, data: EmbeddedDocument) -> List[Dict[str, Any]]:
        """
        格式化输出展示用的输出参数
        :param data: issue的output_params，并非dict
        :return:
        """
        ret = self.to_dict().get("output_params", [])
        for i in ret:
            i["value"] = getattr(data, i["name"], None)
        return ret
