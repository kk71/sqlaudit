# Author: kk.Fang(fkfkbill@gmail.com)

import abc

from core.rule import BaseRuleItem


class BaseIssue(abc.ABC):
    """基础问题"""

    cmdb_id = None  # 纳管库id
    task_record_id = None  # 任务id
    rule = None  # 规则快照（通常考虑除掉code，输出字段）
    output_params = None  # 规则运行输出
    minus_score = None  # 当前规则的扣分，负数
    create_time = None  # 创建时间
    update_time = None  # 修改时间

    @abc.abstractmethod
    def as_issue_of(self, rule: BaseRuleItem):
        """设置当前问题为某个规则的问题"""
        pass
