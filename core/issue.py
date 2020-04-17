# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "BaseIssue",
    "BaseOnlineIssue"
]

import abc

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
    def as_issue_of(self, the_rule: BaseRuleItem, output_data: dict):
        """设置当前问题为某个规则的问题"""
        pass


class BaseOnlineIssue(BaseIssue, SelfCollectingFramework):
    """基础线上审核的问题"""

    entries = None  # 该问题分析时候传入的entries

    # 规则分析的时候接受的entries
    # TODO 子类仅写当前子类需要的entries，如果需要查找当前子类所需的全部entries,
    #      使用inherited_entries查找继承的entries
    ENTRIES = ()

    @classmethod
    def inherited_entries(cls) -> set:
        """不断调用父类的inherited_entries收集所有父类指明的entries"""
        upper_entries = getattr(super(), "inherited_entries", None)
        if upper_entries is not None:
            return set(cls.ENTRIES) | super().inherited_entries()
        return set(cls.ENTRIES)
