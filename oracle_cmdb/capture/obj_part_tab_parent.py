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

    owner = StringField()
    table_name = StringField()
    object_id = IntField()
    data_object_id = IntField()
    partitioning_type = StringField()
    column_name = StringField()
    partitioning_key_count = IntField()
    partition_count = IntField()
    sub_partitioning_key_count = IntField()
    sub_partitioning_type = StringField()
    last_ddl_time = DateTimeField()
    part_role = StringField()
    phy_size_mb = FloatField()

    meta = {
        "collection": "obj_part_tab_parent",
        "indexes": [
            "owner",
            "table_name",
            "object_id",
            "column_name"
        ]
    }

    @classmethod
    def simple_capture(cls, **kwargs) -> str:
        obj_owner: str = kwargs["obj_owner"]
        return f"""
       select /*+opt_param('_optimizer_push_pred_cost_based','FALSE')*/   s.OWNER,
       s.OBJECT_NAME as TABLE_NAME,
       s.OBJECT_ID,
       s.DATA_OBJECT_ID,
       u.partitioning_type,
       t.column_name,
       u.partitioning_key_count,
       u.partition_count,
       u.subpartitioning_key_count as sub_partitioning_key_count,
       u.subpartitioning_type as sub_partitioning_type,
       s.LAST_DDL_TIME,
       'part tab parent' as part_role
  from dba_objects s, Dba_part_key_columns t, dba_part_tables u
 where s.OBJECT_NAME = t.name
   and t.name = u.table_name
   and t.owner = u.owner
   and s.owner = t.owner
   and s.owner = '{obj_owner}'
   and s.SUBOBJECT_NAME is null
   and s.OBJECT_name in (select distinct table_name
                           from dba_tab_partitions t
                          where t.table_owner =  '{obj_owner}')
 order by owner, object_name
"""

    @classmethod
    def post_captured(cls, **kwargs) -> NoReturn:
        SchemaObjectCapturingDoc.post_captured(**kwargs)
        docs: ["ObjPartTabParent"] = kwargs["docs"]
        obj_owner: str = kwargs["obj_owner"]
        cmdb_connector: OraclePlainConnector = kwargs["cmdb_connector"]

        phy_size: [dict] = cmdb_connector.select_dict(f"""
    select segment_name, sum(t.bytes) / 1024 / 1024 as tab_space
    from dba_segments t
    where t.owner = '{obj_owner}'
    and t.partition_name is not null
    and t.segment_name not like 'BIN$%'
    and t.segment_type in ('TABLE SUBPARTITION','TABLE PARTITION')
    group by segment_name
""")
        for doc in docs:
            for a_phy_size in phy_size:
                if doc.table_name == a_phy_size["segment_name"]:
                    doc.phy_size_mb = a_phy_size["tab_space"]
