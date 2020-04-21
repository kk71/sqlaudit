# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "ObjSeqInfo"
]

from typing import NoReturn

from mongoengine import StringField, IntField

from .base import SchemaObjectCapturingDoc


@SchemaObjectCapturingDoc.need_collect()
class ObjSeqInfo(SchemaObjectCapturingDoc):
    """序列信息"""

    min_value = StringField(null=True)
    max_value = StringField(null=True)
    increment_by = IntField(null=True)
    cache_size = IntField(null=True)
    sequence_name = StringField(null=True)
    sequence_owner = StringField(null=True)
    last_number = StringField(null=True)

    meta = {
        "collection": "obj_seq_info",
        "indexes": [
            "sequence_name",
            "sequence_owner"
        ]
    }

    @classmethod
    def simple_capture(cls, **kwargs) -> str:
        obj_owner: str = kwargs["obj_owner"]
        return f"""
            select MIN_VALUE,
                    MAX_VALUE,
                    INCREMENT_BY,
                    CACHE_SIZE, 
                    SEQUENCE_NAME, 
                    sequence_owner, 
                    last_number 
            from dba_sequences where sequence_owner = '{obj_owner}'
        """

    @classmethod
    def post_captured(cls, **kwargs) -> NoReturn:
        SchemaObjectCapturingDoc.post_captured(**kwargs)
        docs: ["ObjSeqInfo"] = kwargs["docs"]
        for d in docs:
            d.min_value = str(d.min_value)
            d.max_value = str(d.max_value)
            d.last_number = str(d.last_number)