# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "Issue"
]

from mongoengine import StringField, FloatField, DictField, IntField, ListField

import core.issue
import rule.rule
import rule.exceptions
import rule.const
from models.mongoengine import *


class Issue(
        BaseDoc,
        core.issue.BaseIssue,
        metaclass=ABCTopLevelDocumentMetaclass):
    """oracle issue"""

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
        "allow_inheritance": True,
        "collection": "issue",
        'indexes': [
            "cmdb_id",
            "db_type",
            "rule_name",
            "entries",
            "level"
        ]
    }

    def as_issue_of(self, the_rule: rule.rule.Rule, output_data: dict):
        """
        作为一个规则的问题
        :param the_rule:
        :param output_data: 规则的运行输出
        :return:
        """
        self.db_type = the_rule.db_type
        self.rule_name = the_rule.name
        self.rule_desc = the_rule.desc
        self.level = the_rule.level
        self.max_score = the_rule.max_score
        self.weight = the_rule.weight
        self.input_params = [i for i in the_rule.to_dict()["input_params"]]
        self.entries = list(the_rule.entries)
        for output_param in the_rule.output_params:
            the_output_data_to_this_param = output_data.get(output_param.name, None)
            if not output_param.validate_data_type(the_output_data_to_this_param):
                raise rule.exceptions.RuleCodeInvalidParamTypeException(
                    f"{str(the_rule)}-{output_param.name}: {the_output_data_to_this_param}")
            self.output_params[output_param.name] = the_output_data_to_this_param

