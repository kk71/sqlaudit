# Author: kk.Fang(fkfkbill@gmail.com)

import abc


class BaseCaptureItem(abc.ABC):
    """基础采集对象"""

    @classmethod
    def simple_capture(cls, *args, **kwargs):
        """简单采集行为"""
        pass

    @classmethod
    def manual_capture(cls, *args, **kwargs):
        """手动采集行为"""
        pass

    @classmethod
    def post_captured(cls, *args, **kwargs):
        """采集后对采到的数据做简单的修正，例如添加辅助字段，修正数据类型等"""
        pass


class BaseCapture(abc.ABC):
    """基础采集"""

    # 采集对象
    capture_items: (BaseCaptureItem,) = ()

    def run(self):
        """启动采集"""
        return
