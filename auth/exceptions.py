# Author: kk.Fang(fkfkbill@gmail.com)


class AuthException(Exception):
    """认证，权限方面的异常"""
    pass


class TokenExpiredException(AuthException):
    """token过期异常"""
    pass

