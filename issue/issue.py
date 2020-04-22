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
import rule.rule
import rule.exceptions
import rule.const
import issue.exceptions
from rule.rule_jar import *
from models.mongoengine import *


class OnlineIssueOutputParams(DynamicEmbeddedDocument):
    """
    线上审核问题输出
    注意：这个嵌入式文档是动态的，意味着字段可以变动。这里只是起到一个提示作用
    """

    # 一句话描述当前问题的关键（应当是人能通读的表述）
    issue_desc = StringField(required=True, default="")

    def as_output_of(
            self,
            the_rule: rule.cmdb_rule.CMDBRule,
            output_data: dict):
        """以给出的数据作为本问题的输出"""
        # 检查实际输出是否包含当前问题要求必须输出的字段。
        for f in self._fields_ordered:
            if f in ("_cls", "_id"):
                continue
            if f not in output_data.keys():
                raise issue.exceptions.IssueBadOutputData(f"need {f}")
        for output_param in the_rule.output_params:
            the_output_data_to_this_param = output_data.get(output_param.name, None)
            # 检查规则的输出参数和实际的输出是否一直，
            if not output_param.validate_data_type(the_output_data_to_this_param):
                raise rule.exceptions.RuleCodeInvalidParamTypeException(
                    f"{str(the_rule)}-{output_param.name}:"
                    f" {the_output_data_to_this_param}")
            setattr(
                self,
                output_param.name,
                the_output_data_to_this_param
            )


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
    input_params = DictField(default=dict)  # 输入参数快照
    output_params = EmbeddedDocumentField(OnlineIssueOutputParams)  # 运行输出
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
