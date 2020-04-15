# Author: kk.Fang(fkfkbill@gmail.com)

import abc
from typing import NoReturn


class BaseCaptureItem(abc.ABC):
    """基础采集对象"""

    cmdb_id = None  # 纳管库id
    task_record_id = None  # 任务id

    MODELS = []  # 需要采集的models

    @classmethod
    def simple_capture(cls, **kwargs) -> str:
        """简单采集行为"""
        pass

    @classmethod
    def manual_capture(cls, **kwargs) -> ["BaseCaptureItem"]:
        """手动采集行为"""
        pass

    @classmethod
    def capture(cls, model_to_capture, **kwargs):
        """采集"""
        pass

    @classmethod
    def post_captured(cls, **kwargs) -> NoReturn:
        """采集后对采到的数据做简单的修正，例如添加辅助字段，修正数据类型等"""
        pass

    @classmethod
    def need_collect(cls):
        def inner(model):
            cls.MODELS.append(model)
            return model
        return inner


class BaseCapture(abc.ABC):
    """基础采集"""

    # 采集目标
    capture_items: (BaseCaptureItem,) = ()

    def run(self):
        """启动采集"""
        return
