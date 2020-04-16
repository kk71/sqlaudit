# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "ObjTabCol"
]

from mongoengine import StringField, IntField, FloatField, DynamicField

from .base import SchemaObjectCapturingDoc


@SchemaObjectCapturingDoc.need_collect()
class ObjTabCol(SchemaObjectCapturingDoc):
    """表列信息"""

    owner = StringField(required=True)
    table_name = StringField(required=True)
    column_id = IntField(required=True)
    column_name = StringField(required=True)
    data_type = StringField(required=True)
    type_change = StringField(required=True)
    nullable = StringField(required=True)
    num_nulls = IntField(required=True)
    num_distinct = IntField(required=True)
    data_default = DynamicField(required=True)
    avg_col_len = FloatField(required=True)

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
