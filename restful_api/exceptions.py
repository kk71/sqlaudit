# Author: kk.Fang(fkfkbill@gmail.com)

from schema import SchemaError


class BaseAPIException(Exception):
    """请求层面的错误"""
    pass


class SchemaErrorWithMessage(BaseAPIException, SchemaError):
    """schema处理的时候抛出异常"""
    pass


