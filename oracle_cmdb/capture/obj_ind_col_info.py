# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "ObjIndColInfo"
]


from mongoengine import StringField, IntField

from .base import *


class ObjIndColInfo(SchemaObjectCapturingDoc):
    """索引列信息"""

    index_owner = StringField()
    index_name = StringField()
    table_owner = StringField()
    table_name = StringField()
    column_name = StringField()
    column_position = IntField()

    meta = {
        "collection": "obj_ind_col_info",
        "indexes": [
            "index_owner",
            "index_name",
            "table_owner",
            "table_name",
            "column_name"
        ]
    }

    @classmethod
    def simple_capture(cls, **kwargs) -> str:
        obj_owner: str = kwargs["obj_owner"]
        return f"""
    select
    INDEX_OWNER,
    INDEX_NAME,
    TABLE_OWNER,
    TABLE_NAME,
    COLUMN_NAME,
    COLUMN_POSITION,
    DESCEND
    from  dba_ind_columns t
    where index_owner='{obj_owner}' and index_name not like '%BIN$%'
    order by t.INDEX_NAME,t.COLUMN_POSITION
"""
