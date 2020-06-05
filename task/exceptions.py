# Author: kk.Fang(fkfkbill@gmail.com)


class TaskException(Exception):
    """任务基础异常"""
    pass


class TaskFailedException(TaskException):
    """任务失败异常"""
    pass


class TaskWaitingTimeoutException(TaskException):
    """任务等待超时异常"""
    pass
