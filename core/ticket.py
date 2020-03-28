# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "BaseTicket",
    "BaseSubTicket",
    "BaseTicketScript"
]

import abc


class BaseTicket(abc.ABC):
    """基础工单"""

    ticket_id = None  # 工单id
    task_name = None  # 工单名
    db_type = None  # 数据库类型
    cmdb_id = None  # 纳管库id
    sub_ticket_count = None  # 总共子工单数
    scripts = None  # 脚本信息
    submit_time = None  # 工单提交时间
    submit_owner = None  # 提交人
    status = None  # 状态
    audit_role_id = None  # 分配审核的角色id
    audit_owner = None  # 实际审核人
    audit_time = None  # 审核时间
    audit_comments = None  # 审核批注
    online_time = None  # 上线时间
    score = None  # 工单评分

    @abc.abstractmethod
    def calculate_score(self, *args, **kwargs):
        """计算工单分数"""
        pass


class BaseTicketScript(abc.ABC):
    """基础工单脚本（即单个脚本文件）"""
    script_id = None  # 脚本id
    script_name = None  # 脚本名
    db_type = None  # 数据库类型
    sub_ticket_count = None  # 子工单数


class BaseSubTicket(abc.ABC):
    """基础子工单（即单条SQL语句）"""
    statement_id = None  # 语句id
    ticket_id = None  # 工单id
    script = None  # 脚本信息
    task_name = None  # 工单名
    db_type = None  # 纳管库类型
    cmdb_id = None  # 纳管库id
    sql_type = None  # 语句类型
    sql_text = None  # 语句原文
    sql_text_no_comment = None  # 无注释语句
    comments = None  # 注释
    position = None  # 该语句在脚本里的位置，从0开始
    static = None  # 静态问题
    dynamic = None  # 动态问题
    error_msg = None  # 错误信息
    check_time = None  # 分析时间

