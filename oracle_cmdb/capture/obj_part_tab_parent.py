# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleObjPartTabParent"
]

from typing import Tuple

from mongoengine import StringField, IntField, DateTimeField, FloatField

from .. import const
from .base import OracleSchemaObjectCapturingDoc


@OracleSchemaObjectCapturingDoc.need_collect()
class OracleObjPartTabParent(OracleSchemaObjectCapturingDoc):
    """分区表父"""

    owner = StringField(null=True)
    table_name = StringField(null=True)
    object_id = IntField(null=True)
    data_object_id = IntField(null=True)
    partitioning_type = StringField(null=True)
    column_name = StringField(null=True)
    partitioning_key_count = IntField(null=True)
    partition_count = IntField(null=True)
    sub_partitioning_key_count = IntField(null=True)
    sub_partitioning_type = StringField(null=True)
    last_ddl_time = DateTimeField(null=True)
    part_role = StringField(null=True)
    phy_size_mb = FloatField(null=True)

    meta = {
        "collection": "oracle_obj_part_tab_parent",
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
select /*+opt_param('_optimizer_push_pred_cost_based','FALSE') gather_plan_statistics */
 s.OWNER,
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
 'part tab parent' as part_role,
 r.bytes / 1024 / 1024 "phy_size_mb"
  from dba_objects          s,
       Dba_part_key_columns t,
       dba_part_tables      u,
       dba_segments         r
 where s.OBJECT_NAME = t.name
   and t.name = u.table_name
   and t.owner = u.owner
   and s.owner = t.owner
   and r.owner = s.owner
   and r.segment_name = s.object_name
   and s.owner = '{obj_owner}'
   and s.SUBOBJECT_NAME is null
   and s.OBJECT_name in (select distinct table_name
                           from dba_tab_partitions t
                          where t.table_owner = '{obj_owner}')
   and r.partition_name is not null
   and r.segment_name not like 'BIN$%'
   and r.segment_type in ('TABLE SUBPARTITION', 'TABLE PARTITION')
 order by owner, object_name
"""

    def get_object_unique_name(self) -> Tuple[str, str, str]:
        return self.owner, const.ORACLE_OBJECT_TYPE_TABLE, self.table_name
