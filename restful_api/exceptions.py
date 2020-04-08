# Author: kk.Fang(fkfkbill@gmail.com)


class BaseAPIException(Exception):
    """请求层面的错误"""
    pass


class SchemaErrorWithMessage(BaseAPIException):
    """schema处理的时候抛出异常"""
    pass


