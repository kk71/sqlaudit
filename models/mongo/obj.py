# Author: kk.Fang(fkfkbill@gmail.com)

from mongoengine import IntField, StringField, ObjectIdField, DateTimeField, \
    BooleanField, DynamicField, FloatField

from .utils import BaseDoc


class ObjTabInfo(BaseDoc):
    """表"""
    _id = ObjectIdField()
    schema_name = StringField("OWNER")
    etl_date = DateTimeField("ETL_DATE")
    ip_address = StringField("IPADDR")
    sid = StringField("SID")
    cmdb_id = IntField()
    record_id = StringField()
    table_name = StringField("TABLE_NAME")
    table_type = StringField("TABLE_TYPE")
    object_type = StringField("OBJECT_TYPE")
    iot_name = StringField("IOT_NAME", null=True)
    num_rows = IntField("NUM_ROWS")
    blocks = IntField("BLOCKS")
    avg_row_len = IntField("AVG_ROW_LEN")
    last_analyzed = DateTimeField("LAST_ANALYZED")
    last_ddl_date = DateTimeField("LAST_DDL_TIME")
    chain_cnt = IntField("CHAIN_CNT")
    partitioned = StringField("PARTITIONED")
    hwm_stat = IntField("HWM_STAT")
    compression = StringField("COMPRESSION")
    phy_size_mb = FloatField("PHY_SIZE(MB)")

    meta = {
        "collection": "obj_tab_info"
    }


class ObjTabCol(BaseDoc):
    """列"""
    _id = ObjectIdField()
    schema_name = StringField("OWNER")
    etl_date = DateTimeField("ETL_DATE")
    ip_address = StringField("IPADDR")
    sid = StringField("SID")
    cmdb_id = IntField()
    record_id = StringField()
    table_name = StringField("TABLE_NAME")
    column_id = IntField("COLUMN_ID")
    column_name = StringField("COLUMN_NAME")
    data_type = StringField("DATA_TYPE")
    type_change = StringField("TYPE_CHANGE")
    nullable = StringField("NULLABLE")
    num_nulls = IntField("NUM_NULLS")
    num_distinct = IntField("NUM_DISTINCT")
    data_default = DynamicField("DATA_DEFAULT", null=True)
    avg_col_len = IntField("AVG_COL_LEN")

    meta = {
        "collection": "obj_tab_col"
    }


class ObjPartTabParent(BaseDoc):
    """父分区表"""
    _id = ObjectIdField()
    schema_name = StringField("OWNER")
    etl_date = DateTimeField("ETL_DATE")
    ip_address = StringField("IPADDR")
    sid = StringField("SID")
    cmdb_id = IntField()
    record_id = StringField()
    table_name = StringField("TABLE_NAME")
    object_id = IntField("OBJECT_ID")
    data_object_id = StringField("DATA_OBJECT_ID", null=True)
    partitioning_type = StringField("PARTITIONING_TYPE")
    column_name = StringField("COLUMN_NAME")
    partitioning_key_count = IntField("PARTITIONING_KEY_COUNT")
    partition_count = IntField("PARTITION_COUNT")
    sub_partitioning_key_count = IntField("SUBPARTITIONING_KEY_COUNT")
    sub_partitioning_type = IntField("SUBPARTITIONING_TYPE")
    last_ddl_date = DateTimeField("LAST_DDL_TIME")
    part_role = StringField("PART_ROLE")
    num_rows = IntField("NUM_ROW")
    phy_size_mb = FloatField("PHY_SIZE(MB)", null=True)    # 少量数据有此字段

    meta = {
        "collection": "obj_part_tab_parent"
    }


class ObjIndColInfo(BaseDoc):
    """索引列"""
    _id = ObjectIdField()
    schema_name = StringField("INDEX_OWNER")
    etl_date = DateTimeField("ETL_DATE")
    ip_address = StringField("IPADDR")
    sid = StringField("SID")
    cmdb_id = IntField()
    record_id = StringField()
    index_name = StringField("INDEX_NAME")
    table_owner = StringField("TABLE_OWNER")
    table_name = StringField("TABLE_NAME")
    column_name = StringField("COLUMN_NAME")
    column_position = IntField("COLUMN_POSITION")
    descend = StringField("DESCEND")

    meta = {
        "collection": "obj_ind_col_info"
    }


class ObjViewInfo(BaseDoc):
    """视图"""
    _id = ObjectIdField()
    obj_pk = StringField("OBJ_PK")
    etl_date = DateTimeField("ETL_DATE")
    ip_address = StringField("IPADDR")
    sid = StringField("SID")
    cmdb_id = IntField()
    record_id = StringField()
    schema_name = StringField("OWNER")
    view_name = StringField("VIEW_NAME")
    object_type = StringField("OBJECT_TYPE")
    referenced_owner = StringField("REFERENCED_OWNER")
    referenced_name = StringField("REFERENCED_NAME")
    referenced_type = StringField("REFERENCED_TYPE")

    meta = {
        "collection": "obj_view_info"
    }

