# Author: kk.Fang(fkfkbill@gmail.com)


class BaseAPIException(Exception):
    """请求层面的错误"""
    pass


class SchemaErrorWithMessageResponsed(BaseAPIException):
    pass


