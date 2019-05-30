# Author: kk.Fang(fkfkbill@gmail.com)

from mongoengine import StringField, IntField, BooleanField, ListField,\
    DictField, FloatField, DateTimeField, LongField, ObjectIdField

from .utils import BaseDoc


class MSQLPlan(BaseDoc):
    _id = ObjectIdField()
    sql_id = StringField("SQL_ID")
    schema = StringField("USERNAME")
    etl_date = DateTimeField("ETL_DATE")
    ipaddr = StringField("IPADDR")
    db_sid = StringField("DB_SID")
    record_id = StringField()
    cmdb_id = IntField()
    plan_hash_value = LongField("PLAN_HASH_VALUE")
    index = IntField("ID")
    depth = IntField("DEPTH")
    parent_id = IntField("PARENT_ID")
    operation = StringField("OPERATION")
    operation_display = StringField("OPERATION_DISPLAY")
    options = StringField("OPTIONS")
    object_node = StringField("OBJECT_NODE", null=True)
    object_owner = StringField("OBJECT_OWNER", null=True)
    object_name = StringField("OBJECT_NAME", null=True)
    object_type = StringField("OBJECT_TYPE", null=True)
    optimizer = StringField("OPTIMIZER", null=True)
    search_columns = IntField("SEARCH_COLUMNS")
    position = IntField("POSITION")
    cost = StringField("COST", null=True)
    cardinality = IntField("CARDINALITY")
    bytes = IntField("BYTES")
    other_tag = StringField("OTHER_TAG", null=True)
    partition_start = StringField("PARTITION_START", null=True)
    partition_stop = StringField("PARTITION_STOP", null=True)
    partition_id = StringField("PARTITION_ID", null=True)
    other = StringField("OTHER", null=True)
    distribution = StringField("DISTRIBUTION", null=True)
    cpu_cost = IntField("CPU_COST", null=True)
    io_cost = IntField(null=True)
    filter_predicates = StringField("FILTER_PREDICATES", null=True)
    access_predicates = StringField("ACCESS_PREDICATES", null=True)
    time = IntField("TIME", null=True)

    meta = {
        "collection": "sqlplan"
    }
