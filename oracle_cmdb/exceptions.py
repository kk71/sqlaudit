# Author: kk.Fang(fkfkbill@gmail.com)


class OracleCMDBException(Exception):
    """oracle纳管库相关的异常"""
    pass


class OracleCMDBBadConfigured(OracleCMDBException):
    """oracle纳管库必须的字段配置错误，或者连接不上"""
    pass
