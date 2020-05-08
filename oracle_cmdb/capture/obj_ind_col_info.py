# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleObjIndColInfo"
]

from mongoengine import StringField, IntField

from .base import OracleSchemaObjectCapturingDoc


@OracleSchemaObjectCapturingDoc.need_collect()
class OracleObjIndColInfo(OracleSchemaObjectCapturingDoc):
    """索引列信息"""

    index_name = StringField(null=True)
    table_owner = StringField(null=True)
    table_name = StringField(null=True)
    column_name = StringField(null=True)
    column_position = IntField(null=True)

    meta = {
        "collection": "oracle_obj_ind_col_info",
        "indexes": [
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
