# Author: kk.Fang(fkfkbill@gmail.com)

from mongoengine import StringField, IntField, BooleanField, ListField,\
    DictField, FloatField, DateTimeField, LongField, ObjectIdField

from .utils import BaseDoc


class MSQLPlan(BaseDoc):
    _id = ObjectIdField()
    sql_id = StringField(db_field="SQL_ID")
    schema = StringField(db_field="USERNAME")
    etl_date = DateTimeField(db_field="ETL_DATE")
    ipaddr = StringField(db_field="IPADDR")
    db_sid = StringField(db_field="DB_SID")
    record_id = StringField()
    cmdb_id = IntField()
    plan_hash_value = LongField(db_field="PLAN_HASH_VALUE")
    index = IntField(db_field="ID")
    depth = IntField(db_field="DEPTH")
    parent_id = IntField(db_field="PARENT_ID")
    operation = StringField(db_field="OPERATION")
    operation_display = StringField(db_field="OPERATION_DISPLAY")
    options = StringField(db_field="OPTIONS")
    object_node = StringField(db_field="OBJECT_NODE", null=True)
    object_owner = StringField(db_field="OBJECT_OWNER", null=True)
    object_name = StringField(db_field="OBJECT_NAME", null=True)
    object_type = StringField(db_field="OBJECT_TYPE", null=True)
    optimizer = StringField(db_field="OPTIMIZER", null=True)
    search_columns = IntField(db_field="SEARCH_COLUMNS")
    position = IntField(db_field="POSITION")
    cost = StringField(db_field="COST", null=True)
    cardinality = IntField(db_field="CARDINALITY")
    bytes = IntField(db_field="BYTES")
    other_tag = StringField(db_field="OTHER_TAG", null=True)
    partition_start = StringField(db_field="PARTITION_START", null=True)
    partition_stop = StringField(db_field="PARTITION_STOP", null=True)
    partition_id = StringField(db_field="PARTITION_ID", null=True)
    other = StringField(db_field="OTHER", null=True)
    distribution = StringField(db_field="DISTRIBUTION", null=True)
    cpu_cost = IntField(db_field="CPU_COST", null=True)
    io_cost = IntField(db_field="IO_COST", null=True)
    filter_predicates = StringField(db_field="FILTER_PREDICATES", null=True)
    access_predicates = StringField(db_field="ACCESS_PREDICATES", null=True)
    time = IntField(db_field="TIME", null=True)

    meta = {
        "collection": "sqlplan"
    }
