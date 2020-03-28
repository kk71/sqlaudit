# Author: kk.Fang(fkfkbill@gmail.com)

import abc
from typing import NoReturn


class BaseRestfulRequestHandler(abc.ABC):
    """基础restful请求处理"""

    @abc.abstractmethod
    def get_json_args(self, *args, **kwargs):
        """获取非get请求情况下，http request body所带的json数据"""
        pass

    @abc.abstractmethod
    def get_query_args(self, *args, **kwargs):
        """获取query string所带的mapping数据"""
        pass

    @abc.abstractmethod
    def resp(self, *args, **kwargs) -> NoReturn:
        """返回接口"""
        pass
