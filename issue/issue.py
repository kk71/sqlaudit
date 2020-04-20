# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OnlineIssue"
]

from typing import Union, Generator

from mongoengine import StringField, FloatField, DictField, IntField, ListField

import core.issue
import rule.rule
import rule.exceptions
import rule.const
from rule.rule_jar import *
from models.mongoengine import *


class OnlineIssue(
        BaseDoc,
        core.issue.BaseOnlineIssue,
        metaclass=ABCTopLevelDocumentMetaclass):
    """common online issue"""

    cmdb_id = IntField(required=True)
    db_type = StringField(required=True)
    rule_name = StringField(required=True)
    rule_desc = StringField(required=True)
    entries = ListField(default=lambda: [], choices=rule.const.ALL_RULE_ENTRIES)
    weight = FloatField()
    max_score = FloatField(required=True)
    input_params = DictField(default=lambda: {})  # 输入参数快照
    output_params = DictField(default=lambda: {})  # 运行输出
    minus_score = FloatField(default=0)  # 当前规则的扣分，负数
    level = IntField()  # 规则优先级

    meta = {
        "abstract": True,
        'indexes': [
            "cmdb_id",
            "rule_name",
            "entries",
            "level"
        ]
    }

    # 规定使用的规则entries为线上审核
    ENTRIES = (rule.const.RULE_ENTRY_ONLINE,)

    @classmethod
    def generate_rule_jar(cls,
                          db_type: str,
                          entries: [str] = None,
                          **kwargs) -> RuleJar:
        if entries is None:
            entries = cls.inherited_entries()
        return RuleJar.gen_jar_with_entries(*entries, db_type=db_type, **kwargs)

    def as_issue_of(self,
                    the_rule: rule.rule.CMDBRule,
                    output_data: dict,
                    minus_score: Union[int, float], **kwargs):
        """
        作为一个规则的问题
        :param the_rule:
        :param output_data: 规则的运行输出
        :param minus_score
        :return:
        """
        self.cmdb_id = the_rule.cmdb_id
        self.db_type = the_rule.db_type
        self.rule_name = the_rule.name
        self.rule_desc = the_rule.desc
        self.level = the_rule.level
        self.max_score = the_rule.max_score
        self.weight = the_rule.weight
        self.input_params = [i for i in the_rule.to_dict()["input_params"]]
        self.entries = list(the_rule.entries)
        self.minus_score = minus_score
        for output_param in the_rule.output_params:
            the_output_data_to_this_param = output_data.get(output_param.name, None)
            if not output_param.validate_data_type(the_output_data_to_this_param):
                raise rule.exceptions.RuleCodeInvalidParamTypeException(
                    f"{str(the_rule)}-{output_param.name}: {the_output_data_to_this_param}")
            self.output_params[output_param.name] = the_output_data_to_this_param

    @classmethod
    def simple_analyse(cls, **kwargs) -> Generator["OnlineIssue"]:
        pass

    @classmethod
    def filter_with_entries(cls, *args, **kwargs):
        """便于子类只查询当前（以及其子类）的对象"""
        return cls.objects(
            entries__all=cls.inherited_entries()).filter(*args, **kwargs)
