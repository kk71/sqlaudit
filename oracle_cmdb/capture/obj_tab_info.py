# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleObjTabInfo"
]

from typing import Tuple

from mongoengine import StringField, IntField, DateTimeField, FloatField

from .. import const
from .base import OracleSchemaObjectCapturingDoc


@OracleSchemaObjectCapturingDoc.need_collect()
class OracleObjTabInfo(OracleSchemaObjectCapturingDoc):
    """表信息"""

    owner = StringField(null=True)
    table_name = StringField(null=True)
    table_type = StringField(null=True)
    object_type = StringField(null=True)
    iot_name = StringField(null=True)
    num_rows = IntField(null=True)
    blocks = IntField(null=True)
    avg_row_len = IntField(null=True)
    last_analyzed = DateTimeField(null=True)
    last_ddl_time = DateTimeField(null=True)
    chain_cnt = IntField(null=True)
    partitioned = StringField(null=True)
    hwm_stat = IntField(null=True)
    compression = StringField(null=True)
    phy_size_mb = FloatField(null=True)

    meta = {
        "collection": "oracle_obj_tab_info",
        "indexes": [
            "owner",
            "table_name"
        ]
    }

    @classmethod
    def simple_capture(cls, **kwargs) -> str:
        obj_owner: str = kwargs["obj_owner"]
        return f"""
select t.owner,
       t.table_name,
       decode(t.partitioned,
              'YES',
              'PART',
              decode(t.temporary,
                     'Y',
                     'TEMP',
                     decode(t.iot_type, 'IOT', 'IOT', 'NORMAL'))) table_type,
       s.object_type,
       t.iot_name,
       t.NUM_ROWS,
       t.BLOCKS,
       t.AVG_ROW_LEN,
       t.LAST_ANALYZED,
       s.last_ddl_time,
       t.CHAIN_CNT,
       t.PARTITIONED,
       trunc(((t.AVG_ROW_LEN * t.NUM_ROWS) / 8) /
             (decode(t.BLOCKS, 0, 1, t.BLOCKS)) * 100) as HWM_STAT,
       t.COMPRESSION,
       p.bytes/1024/1024 phy_size_mb
  from dba_tables t, dba_objects s, dba_segments p
 where t.table_name = s.object_name
   and p.segment_name = t.table_name
   and t.owner = s.owner
   and s.object_type = 'TABLE'
   and t.table_name not like '%BIN$%'
   and t.owner = '{obj_owner}'
"""

    def get_object_unique_name(self) -> Tuple[str, str, str]:
        return self.owner, const.ORACLE_OBJECT_TYPE_TABLE, self.table_name
