# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "ObjTabCol"
]

from mongoengine import StringField, IntField, FloatField, DynamicField

from .base import *


class ObjTabCol(SchemaObjectCapturingDoc):
    """表列信息"""

    owner = StringField()
    table_name = StringField()
    column_id = IntField()
    column_name = StringField()
    data_type = StringField()
    type_change = StringField()
    nullable = StringField()
    num_nulls = IntField()
    num_distinct = IntField()
    data_default = DynamicField()
    avg_col_len = FloatField()

    meta = {
        "collection": "obj_tab_col",
        "indexes": [
            "owner",
            "table_name",
            "column_id",
            "column_name"
        ]
    }

    @classmethod
    def simple_capture(cls, **kwargs) -> str:
        obj_owner: str = kwargs["obj_owner"]
        return f"""
     select t.OWNER,
       t.TABLE_NAME,
       t.COLUMN_ID,
       t.COLUMN_NAME,
       t.DATA_TYPE,
       '' TYPE_CHANGE,
       t.NULLABLE,
       t.NUM_NULLS,
       t.NUM_DISTINCT,
       t.DATA_DEFAULT,
       t.AVG_COL_LEN
  from dba_tab_columns t, dba_objects s
 where t.OWNER = s.owner
   and t.TABLE_NAME = s.object_name
   and t.OWNER = '{obj_owner}'
   and s.OBJECT_TYPE='TABLE'
   order by t.TABLE_NAME,t.COLUMN_NAME
"""
