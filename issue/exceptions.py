# Author: kk.Fang(fkfkbill@gmail.com)


class IssueException(Exception):
    """问题异常"""
    pass


class IssueBadOutputData(IssueException):
    """问题的输出数据与预期不符"""
    pass
