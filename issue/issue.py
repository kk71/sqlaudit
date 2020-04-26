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
        """
        检查对应的规则的输出参数是否满足当前issue的要求
        TODO 这个检查只是检查规则输出字段是否包含的当前issue输出要求的字段，
             但是并不强制规则的输出字段为必选
             因为规则有一部分是跟工单共用的，你不能要求工单运行规则的时候也必须输出这些字段
             因此，这个检查是不能保证分析的时候确实拿到这些必须的字段的
        :param the_rule:
        :return:
        """
        keys_of_rule_output_params = {
            i.name
            for i in the_rule.output_params
        }
        for f in self._fields_ordered:
            if f in ("_cls", "_id"):
                continue
            if f not in keys_of_rule_output_params:
                raise issue.exceptions.IssueBadOutputData(
                    f"{the_rule}: requires {f}, make sure the output parameter is set")

    def as_output_of(
            self,
            output_data: dict):
        """以给出的数据作为本问题的输出"""
        for f in self._fields_ordered:
            if f in ("_cls", "_id"):
                continue
            if f not in output_data.keys():
                raise issue.exceptions.IssueBadOutputData(
                    f"{self.__class__} requires {f}: {output_data}")
        for k, v in output_data.items():
            setattr(self, k, v)


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
                          **kwargs) -> RuleJar:
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
        self.output_params.as_output_of(output_data)

    @classmethod
    def simple_analyse(cls, **kwargs) -> Generator["OnlineIssue", None, None]:
        pass

    @classmethod
    def check_rule_output_and_issue(cls, **kwargs):
        """
        检查规则和对应的输出字段是否符合要求
        TODO 这个检查只是在检查代码的时候需要运行，不要在生产环境中运行
        :param kwargs:
        :return:
        """
        print(f"{cls}: checking rule output to issue output...")
        rule_jar = cls.generate_rule_jar(**kwargs)
        for the_rule in rule_jar:
            cls().output_params.check_rule_output_and_issue(the_rule)

    @classmethod
    def filter_with_inherited_entries(cls, *args, **kwargs):
        """
        默认的mongoengine document继承，是带有_cls的。
        所以不需要根据entries去过滤
        {$all: [...]}的过滤速度比{$in: [...]}要慢。
        特殊情况下可以用entries去过滤
        :param args:
        :param kwargs:
        :return:
        """
        return super().filter(
            entries__all=cls.INHERITED_ENTRIES).filter(*args, **kwargs)

    @classmethod
    def filter_with_entries(cls, *args, **kwargs):
        return super().filter(
            entries__all=cls.ENTRIES).filter(*args, **kwargs)
