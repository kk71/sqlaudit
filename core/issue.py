# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "BaseIssue",
    "BaseOnlineIssue"
]

import abc
from typing import Union, NoReturn, List

from .rule import BaseRuleItem
from .capture import BaseCaptureItem
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

    # 当前类所在的规则分析的时候接受的entries
    ENTRIES: tuple = ()

    # 当前类收集到的全部entries的集合（实际类型是tuple）
    COLLECTED_ENTRIES_SET: tuple = None

    # 当前类以及父类继承过来的entries集合（实际类型是tuple）
    INHERITED_ENTRIES: tuple = None

    # 当前类相关的采集（实际类型是tuple）
    RELATED_CAPTURE: tuple = ()

    @classmethod
    def simple_analyse(cls, **kwargs):
        """简单规则分析"""
        pass

    @classmethod
    def post_analysed(cls, **kwargs) -> NoReturn:
        """规则分析结束后处理"""
        pass

    @classmethod
    def collect(cls):
        super().collect()
        entries_set = set()
        for collected_models in cls.COLLECTED:
            entries_set.update(collected_models.ENTRIES)
        cls.COLLECTED_ENTRIES_SET = tuple(entries_set)

    @classmethod
    def related_capture(cls, entries: List[str]) -> List[BaseCaptureItem]:
        """找到issue相关的capture"""
        assert cls.ALL_SUB_CLASSES
        related_captures = []
        for i in cls.ALL_SUB_CLASSES:
            if set(i.INHERITED_ENTRIES).issuperset(entries):
                for j in i.RELATED_CAPTURE:
                    if j not in related_captures:
                        related_captures.append(j)
        return related_captures
