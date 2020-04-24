# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OnlineIssue",
    "OnlineIssueOutputParams"
]

from typing import Union, Generator

from mongoengine import StringField, FloatField, DictField, IntField,\
    ListField, EmbeddedDocumentField, DynamicEmbeddedDocument

import core.issue
import rule.cmdb_rule
import rule.const
import issue.exceptions
from rule.rule_jar import *
from models.mongoengine import *


class OnlineIssueOutputParams(DynamicEmbeddedDocument):
    """
    线上审核问题输出
    注意：这个嵌入式文档是动态的，意味着字段可以变动。这里只是起到一个提示作用
    """
    meta = {
        "allow_inheritance": True
    }

    def check_rule_output_and_issue(self, the_rule: rule.cmdb_rule.CMDBRule):
        """检查对应的规则的输出参数是否满足当前issue的要求"""
        keys_of_rule_output_params = {i.name for i in the_rule.output_params}
        for f in self._fields_ordered:
            if f in ("_cls", "_id"):
                continue
            if f not in keys_of_rule_output_params:
                raise issue.exceptions.IssueBadOutputData(f"{the_rule}: need {f}")

    def as_output_of(
            self,
            the_rule: rule.cmdb_rule.CMDBRule,
            output_data: dict):
        """以给出的数据作为本问题的输出"""
        for output_param in the_rule.output_params:
            the_output_data_to_this_param = output_data.get(output_param.name, None)
            # 检查规则的输出参数和实际的输出是否一直，
            output_param.validate_data_type(the_output_data_to_this_param)
            setattr(
                self,
                output_param.name,
                the_output_data_to_this_param
            )


class IssueMetaABCMetaTopLevelDocMeta(
        ABCTopLevelDocumentMetaclass,
        core.issue.BaseOnlineIssueMetaclassWithABCMetaClass):
    pass


class OnlineIssue(
        BaseDoc,
        core.issue.BaseOnlineIssue,
        metaclass=IssueMetaABCMetaTopLevelDocMeta):
    """common online issue"""

    cmdb_id = IntField(required=True)
    db_type = StringField(required=True)
    rule_name = StringField(required=True)
    rule_desc = StringField(required=True)
    entries = ListField(default=lambda: [], choices=rule.const.ALL_RULE_ENTRIES)
    weight = FloatField()
    max_score = FloatField(required=True)
    input_params = DictField(default=dict)  # 输入参数快照
    output_params = EmbeddedDocumentField(
        OnlineIssueOutputParams, default=OnlineIssueOutputParams)  # 运行输出
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
                          cmdb_id: int,
                          entries: [str] = None,
                          **kwargs) -> RuleJar:
        if entries is None:
            entries = cls.INHERITED_ENTRIES
        return RuleJar.gen_jar_with_entries(*entries, cmdb_id=cmdb_id, **kwargs)

    def as_issue_of(self,
                    the_rule: rule.cmdb_rule.CMDBRule,
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
        self.input_params = {
            i["name"]: i["value"]
            for i in the_rule.to_dict()["input_params"]
        }
        self.entries = list(the_rule.entries)
        self.minus_score = minus_score
        self.output_params.as_output_of(the_rule, output_data)

    @classmethod
    def simple_analyse(cls, **kwargs) -> Generator["OnlineIssue", None, None]:
        pass

    @classmethod
    def filter_with_entries(cls, *args, **kwargs):
        """便于子类只查询当前（以及其子类）的对象"""
        return cls.objects(
            entries__all=cls.inherited_entries()).filter(*args, **kwargs)

    @classmethod
    def check_rule_output_and_issue(cls, **kwargs):
        """检查规则和对应的输出字段是否符合要求"""
        print(f"{cls.__doc__}: checking rule output to issue output...")
        rule_jar = cls.generate_rule_jar(**kwargs)
        for the_rule in rule_jar:
            cls().output_params.check_rule_output_and_issue(the_rule)
