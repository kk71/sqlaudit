# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "BaseIssue"
]

import abc

from core.rule import BaseRuleItem


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
