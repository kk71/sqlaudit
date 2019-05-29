# Author: kk.Fang(fkfkbill@gmail.com)

from mongoengine import IntField, StringField, ObjectIdField, DateTimeField, \
    BooleanField, DynamicField, FloatField

from .utils import BaseDoc


class ObjTabInfo(BaseDoc):
    """表"""
    _id = ObjectIdField()
    schema_name = StringField()
    etl_date = DateTimeField()
    ip_address = StringField()  # TODO
    sid = StringField()
    cmdb_id = IntField()
    record_id = StringField()
    table_name = StringField()
    table_type = StringField()
    object_type = StringField()
    iot_name = StringField()
    num_rows = IntField()
    blocks = IntField()
    avg_row_len = IntField()
    last_analyzed = DateTimeField()
    last_ddl_date = DateTimeField()  # TODO
    chain_cnt = IntField()
    partitioned = StringField()
    hwm_stat = IntField()
    compression = StringField()
    phy_size_mb = FloatField(help_text="MB")

    meta = {
        "collection": "obj_tab_info"
    }


class ObjTabCol(BaseDoc):
    """列"""
    _id = ObjectIdField()
    schema_name = StringField()
    etl_date = DateTimeField()
    ip_address = StringField()  # TODO
    sid = StringField()
    cmdb_id = IntField()
    record_id = StringField()
    table_name = StringField()
    column_id = IntField()
    column_name = StringField()
    data_type = StringField()
    type_change = StringField()
    nullable = StringField()
    num_nulls = IntField()
    num_distinct = IntField()
    data_default = DynamicField()
    avg_col_len = IntField()

    meta = {
        "collection": "obj_tab_col"
    }


class ObjPartTabParent(BaseDoc):
    """父分区表"""
    _id = ObjectIdField()
    schema_name = StringField(help_text="table owner")
    etl_date = DateTimeField()
    ip_address = StringField()  # TODO
    sid = StringField()
    cmdb_id = IntField()
    record_id = StringField()
    table_name = StringField()
    object_id = IntField()
    data_object_id = StringField()
    partition_type = StringField()
    column_name = StringField()
    partition_key_count = IntField()
    partition_count = IntField()
    sub_partitioning_key_count = IntField()
    sub_partitioning_type = IntField()
    last_ddl_date = DateTimeField()
    part_role = StringField()
    num_row = IntField()
    phy_size_mb = FloatField(help_text="MB")  # 少量数据有此字段

    meta = {
        "collection": "obj_part_tab_parent"
    }


class ObjIndColInfo(BaseDoc):
    """索引列"""
    _id = ObjectIdField()
    schema_name = StringField(help_text="the owner of this index")
    etl_date = DateTimeField()
    ip_address = StringField()  # TODO
    sid = StringField()
    cmdb_id = IntField()
    record_id = StringField()
    index_name = StringField()
    table_owner = StringField()
    table_name = StringField()
    column_name = StringField()
    column_position = IntField()
    descend = StringField()

    meta = {
        "collection": "obj_ind_col_info"
    }


class ObjViewInfo(BaseDoc):
    """视图"""
    _id = ObjectIdField()
    obj_pk = StringField()
    etl_date = DateTimeField()
    ip_address = StringField()  # TODO
    sid = StringField()
    cmdb_id = IntField()
    record_id = StringField()
    schema_name = StringField()
    view_name = StringField()
    object_type = StringField()
    referenced_owner = StringField()
    referenced_name = StringField()
    referenced_type = StringField()

    meta = {
        "collection": "obj_view_info"
    }

