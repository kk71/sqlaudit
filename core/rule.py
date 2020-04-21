# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "BaseRuleItem",
    "BaseRuleJar"
]

import abc
from typing import Union


class BaseRuleItem(metaclass=abc.ABCMeta):
    """基础规则对象"""

    name = None  # 规则名称
    desc = None  # 规则描述
    db_type = None  # 数据库类型
    entries = None  # 规则代码可以接受的运行场景。
    input_params = None  # 输入参数
    output_params = None  # 输出参数
    weight = None  # 权重
    max_score = None  # 最高分
    code = None  # 规则的python代码
    status = None  # 规则是否启用
    solution = None  # 解决方案描述
    create_time = None  # 创建时间
    update_time = None  # 修改时间

    UNIQUE_KEYS = ()

    def __init__(self, *args, **kwargs):
        self._code = None  # 已构建的规则python函数引用

    @abc.abstractmethod
    def construct_code(self, *args, **kwargs):
        """构建规则函数"""
        self._code = None

    @abc.abstractmethod
    def run(self, *args, **kwargs) -> [(Union[int, float], dict)]:
        """执行规则"""
        pass

    @staticmethod
    @abc.abstractmethod
    def code_template():
        """返回code的模板"""
        pass

    @abc.abstractmethod
    def unique_key(self, as_dict=False) -> Union[tuple, dict]:
        """返回一个规则的唯一标识"""
        unique_keys = tuple([getattr(self, i) for i in self.UNIQUE_KEYS])
        if not as_dict:
            return unique_keys
        return dict(zip(self.UNIQUE_KEYS, unique_keys))


class BaseRuleJar(abc.ABC):
    """规则暂存仓库"""

    @abc.abstractmethod
    def __init__(self, *args, **kwargs):
        self.rules = []

    def get_rules(self):
        """返回规则们"""
        return self.rules
