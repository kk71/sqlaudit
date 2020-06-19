# Author: kk.Fang(fkfkbill@gmail.com)


class BaseTicketException(Exception):
    pass


class TicketNotFound(BaseTicketException):
    """工单未找到"""
    pass


class TicketWithWrongStatus(BaseTicketException):
    """工单处于错误的状态，不能执行某些操作"""
    pass


class TicketAnalyseException(BaseTicketException):
    """线下工单分析的时候出错，导致部分子工单不能生成"""
    pass


class NoSubTicketGenerated(BaseTicketException):
    """没有任何子工单"""
    pass


class TicketManualAuditWithWrongRole(BaseTicketException):
    """使用错误的角色进行了工单人工审核"""
    pass
