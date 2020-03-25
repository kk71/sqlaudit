# Author: kk.Fang(fkfkbill@gmail.com)


class TicketNotFound(Exception):
    """工单未找到"""
    pass


class TicketWithWrongStatus(Exception):
    """工单处于错误的状态，不能执行某些操作"""
    pass


class TicketAnalyseException(Exception):
    """线下工单分析的时候出错，导致部分子工单不能生成"""
    pass


class NoEnabledRuleToCalculateScore(Exception):
    """计算工单分数失败，没有任何子工单"""
    pass
