# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "ObjPartTabParent"
]

from typing import NoReturn

from mongoengine import StringField, IntField, DateTimeField, FloatField

from .base import *
from ..plain_db import OraclePlainConnector


class ObjPartTabParent(SchemaObjectCapturingDoc):
    """分区表父"""

    @classmethod
    def simple_capture(cls, **kwargs) -> str:
        obj_owner: str = kwargs["obj_owner"]
        return f"""
"""

    @classmethod
    def post_captured(cls, **kwargs) -> NoReturn:
        pass