# Author: kk.Fang(fkfkbill@gmail.com)

from mongoengine import IntField, StringField, ObjectIdField, DateTimeField, \
    BooleanField, DynamicField, FloatField

from .utils import BaseDoc


class ObjectBaseDoc(BaseDoc):

    meta = {
        'abstract': True,
    }

    @classmethod
    def filter_by_exec_hist(cls, exec_history_object):
        """按照record_id查询"""
        return cls.objects.filter(record_id__startswith=str(exec_history_object.id))


class ObjTabInfo(ObjectBaseDoc):
    """表"""
    _id = ObjectIdField()
    schema_name = StringField(db_field="OWNER")
    etl_date = DateTimeField(db_field="ETL_DATE")
    ip_address = StringField(db_field="IPADDR")
    sid = StringField(db_field="SID")
    cmdb_id = IntField()
    record_id = StringField()
    table_name = StringField(db_field="TABLE_NAME")
    table_type = StringField(db_field="TABLE_TYPE")
    object_type = StringField(db_field="OBJECT_TYPE")
    iot_name = StringField(db_field="IOT_NAME", null=True)
    num_rows = IntField(db_field="NUM_ROWS")
    blocks = IntField(db_field="BLOCKS")
    avg_row_len = IntField(db_field="AVG_ROW_LEN")
    last_analyzed = DateTimeField(db_field="LAST_ANALYZED")
    last_ddl_date = DateTimeField(db_field="LAST_DDL_TIME")
    chain_cnt = IntField(db_field="CHAIN_CNT")
    partitioned = StringField(db_field="PARTITIONED")
    hwm_stat = IntField(db_field="HWM_STAT")
    compression = StringField(db_field="COMPRESSION")
    phy_size_mb = FloatField(db_field="PHY_SIZE(MB)")

    meta = {
        "collection": "obj_tab_info"
    }


class ObjTabCol(ObjectBaseDoc):
    """列"""
    _id = ObjectIdField()
    schema_name = StringField(db_field="OWNER")
    etl_date = DateTimeField(db_field="ETL_DATE")
    ip_address = StringField(db_field="IPADDR")
    sid = StringField(db_field="SID")
    cmdb_id = IntField()
    record_id = StringField()
    table_name = StringField(db_field="TABLE_NAME")
    column_id = IntField(db_field="COLUMN_ID")
    column_name = StringField(db_field="COLUMN_NAME")
    data_type = StringField(db_field="DATA_TYPE")
    type_change = StringField(db_field="TYPE_CHANGE")
    nullable = StringField(db_field="NULLABLE")
    num_nulls = IntField(db_field="NUM_NULLS")
    num_distinct = IntField(db_field="NUM_DISTINCT")
    data_default = DynamicField(db_field="DATA_DEFAULT", null=True)
    avg_col_len = IntField(db_field="AVG_COL_LEN")

    meta = {
        "collection": "obj_tab_col"
    }


class ObjPartTabParent(ObjectBaseDoc):
    """父分区表"""
    _id = ObjectIdField()
    schema_name = StringField(db_field="OWNER")
    etl_date = DateTimeField(db_field="ETL_DATE")
    ip_address = StringField(db_field="IPADDR")
    sid = StringField(db_field="SID")
    cmdb_id = IntField()
    record_id = StringField()
    table_name = StringField(db_field="TABLE_NAME")
    object_id = IntField(db_field="OBJECT_ID")
    data_object_id = StringField(db_field="DATA_OBJECT_ID", null=True)
    partitioning_type = StringField(db_field="PARTITIONING_TYPE")
    column_name = StringField(db_field="COLUMN_NAME")
    partitioning_key_count = IntField(db_field="PARTITIONING_KEY_COUNT")
    partition_count = IntField(db_field="PARTITION_COUNT")
    sub_partitioning_key_count = IntField(db_field="SUBPARTITIONING_KEY_COUNT")
    sub_partitioning_type = IntField(db_field="SUBPARTITIONING_TYPE")
    last_ddl_date = DateTimeField(db_field="LAST_DDL_TIME")
    part_role = StringField(db_field="PART_ROLE")
    num_rows = IntField(db_field="NUM_ROW")
    phy_size_mb = FloatField(db_field="PHY_SIZE(MB)", null=True)    # 少量数据有此字段

    meta = {
        "collection": "obj_part_tab_parent"
    }


class ObjIndColInfo(ObjectBaseDoc):
    """索引列"""
    _id = ObjectIdField()
    schema_name = StringField(db_field="INDEX_OWNER")
    etl_date = DateTimeField(db_field="ETL_DATE")
    ip_address = StringField(db_field="IPADDR")
    sid = StringField(db_field="SID")
    cmdb_id = IntField()
    record_id = StringField()
    index_name = StringField(db_field="INDEX_NAME")
    table_owner = StringField(db_field="TABLE_OWNER")
    table_name = StringField(db_field="TABLE_NAME")
    column_name = StringField(db_field="COLUMN_NAME")
    column_position = IntField(db_field="COLUMN_POSITION")
    descend = StringField(db_field="DESCEND")

    meta = {
        "collection": "obj_ind_col_info"
    }


class ObjViewInfo(ObjectBaseDoc):
    """视图"""
    _id = ObjectIdField()
    obj_pk = StringField(db_field="OBJ_PK")
    etl_date = DateTimeField(db_field="ETL_DATE")
    ip_address = StringField(db_field="IPADDR")
    sid = StringField(db_field="SID")
    cmdb_id = IntField()
    record_id = StringField()
    schema_name = StringField(db_field="OWNER")
    view_name = StringField(db_field="VIEW_NAME")
    object_type = StringField(db_field="OBJECT_TYPE")
    referenced_owner = StringField(db_field="REFERENCED_OWNER")
    referenced_name = StringField(db_field="REFERENCED_NAME")
    referenced_type = StringField(db_field="REFERENCED_TYPE")

    meta = {
        "collection": "obj_view_info"
    }

