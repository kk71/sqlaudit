# Author: kk.Fang(fkfkbill@gmail.com)


class RuleException(Exception):
    """规则异常"""
    pass


class RuleCodeException(RuleException):
    """规则代码层面的异常"""


class RuleCodeInvalidException(RuleCodeException):
    """规则代码创建失败，或者无法执行"""
    pass


class RuleCodeInvalidParamTypeException(RuleCodeException):
    """规则输入输出参数与配置的不符"""
    pass


class RuleCodeInvalidReturnException(RuleCodeException):
    """规则代码返回不正确"""
    pass
