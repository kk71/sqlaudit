# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "ObjTabInfo"
]

from typing import NoReturn

from mongoengine import StringField, IntField, DateTimeField, FloatField

from .base import *
from ..plain_db import OraclePlainConnector


class ObjTabInfo(SchemaObjectCapturingDoc):
    """表信息"""

    owner = StringField()
    table_name = StringField()
    table_type = StringField()
    object_type = StringField()
    iot_name = StringField()
    num_rows = IntField()
    blocks = IntField()
    avg_row_len = IntField()
    last_analysed = DateTimeField()
    last_ddl_time = DateTimeField()
    chain_cnt = IntField()
    partitioned = StringField()
    hwm_stat = IntField()
    compression = StringField()
    phy_size_mb = FloatField()

    meta = {
        "collection": "obj_tab_info",
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
                  decode(
                        t.temporary, 'Y', 'TEMP',
                        decode (t.iot_type,'IOT','IOT','NORMAL'))
                  ) table_type,
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
           t.COMPRESSION
      from dba_tables t, dba_objects s
     where t.table_name = s.object_name
       and t.owner = s.owner
       and s.object_type = 'TABLE'
       and t.table_name not like '%BIN$%'
       and t.owner = '{obj_owner}'
"""

    @classmethod
    def post_captured(cls, **kwargs) -> NoReturn:
        SchemaObjectCapturingDoc.post_captured(**kwargs)
        docs: ["ObjTabInfo"] = kwargs["docs"]
        obj_owner: str = kwargs["obj_owner"]
        cmdb_connector: OraclePlainConnector = kwargs["cmdb_connector"]

        phy_size: dict = cmdb_connector.select_dict(f"""
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
