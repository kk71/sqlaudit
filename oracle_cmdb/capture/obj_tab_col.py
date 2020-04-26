# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleObjTabCol"
]

from mongoengine import StringField, IntField, FloatField, DynamicField

from .base import SchemaObjectCapturingDoc


@SchemaObjectCapturingDoc.need_collect()
class OracleObjTabCol(SchemaObjectCapturingDoc):
    """表列信息"""

    owner = StringField(null=True)
    table_name = StringField(null=True)
    column_id = IntField(null=True)
    column_name = StringField(null=True)
    data_type = StringField(null=True)
    type_change = StringField(null=True)
    nullable = StringField(null=True)
    num_nulls = IntField(null=True)
    num_distinct = IntField(null=True)
    data_default = DynamicField(null=True)
    avg_col_len = FloatField(null=True)

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
