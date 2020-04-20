# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "BaseCaptureItem"
]

from typing import NoReturn

from .self_collecting_class import *


class BaseCaptureItem(SelfCollectingFramework):
    """采集"""

    cmdb_id = None  # 纳管库id
    task_record_id = None  # 任务id

    @classmethod
    def simple_capture(cls, **kwargs) -> str:
        """简单采集行为"""
        pass

    @classmethod
    def post_captured(cls, **kwargs) -> NoReturn:
        """采集后对采到的数据做简单的修正，例如添加辅助字段，修正数据类型等"""
        pass
