# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "BaseIssue",
    "BaseOnlineIssue"
]

import abc
from typing import Union, NoReturn

from .rule import BaseRuleItem
from .self_collecting_class import *


class BaseIssue(abc.ABC):
    """基础问题"""

    db_type = None  # 纳管库类型
    cmdb_id = None  # 纳管库id
    rule_name = None  # 规则名称
    rule_desc = None  # 规则描述
    input_params = None  # 输入参数快照
    output_params = None  # 规则运行输出
    minus_score = None  # 当前规则的扣分，负数
    weight = None  # 权重快照
    max_score = None  # 最大扣分快照

    @abc.abstractmethod
    def as_issue_of(self,
                    the_rule: BaseRuleItem,
                    output_data: dict,
                    minus_score: Union[int, float], **kwargs):
        """设置当前问题为某个规则的问题"""
        pass


class BaseOnlineIssueMetaClass(type):
    """元类：基础线上审核的问题"""

    def __init__(cls, name, bases, attrs):

        super().__init__(name, bases, attrs)

        # 构建inherited_entries
        inherited_entries_key = "INHERITED_ENTRIES"
        inherited_entries = []
        for b in bases:
            inherited_entries_of_b = getattr(b, inherited_entries_key, None)
            if inherited_entries_of_b is not None:
                inherited_entries = list(
                    set(inherited_entries) | set(inherited_entries_of_b))
        current_entries = getattr(cls, "ENTRIES", ())
        setattr(
            cls,
            inherited_entries_key,
            tuple(set(inherited_entries + list(current_entries)))
        )


class BaseOnlineIssueMetaclassWithSelfCollectingMeta(
        BaseOnlineIssueMetaClass, SelfCollectingFrameworkMeta):
    pass


class BaseOnlineIssue(
        BaseIssue,
        SelfCollectingFramework,
        metaclass=BaseOnlineIssueMetaclassWithSelfCollectingMeta):
    """基础线上审核的问题"""

    entries = None  # 该问题分析时候传入的entries

    # 规则分析的时候接受的entries
    # TODO 子类仅填写当前子类需要的entries，如果需要查找当前子类所需的全部entries,
    #      使用inherited_entries查找继承的entries
    ENTRIES = ()

    @classmethod
    def simple_analyse(cls, **kwargs):
        """简单规则分析"""
        pass

    @classmethod
    def post_analysed(cls, **kwargs) -> NoReturn:
        """规则分析结束后处理"""
        pass
