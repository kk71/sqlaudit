# Author: kk.Fang(fkfkbill@gmail.com)

from mongoengine import StringField, IntField, BooleanField, ListField,\
    DictField, FloatField, DateTimeField, LongField

from .utils import BaseDoc


class MSQLPlan(BaseDoc):
    statement_id = StringField()
    schema = StringField()
    etl_date = DateTimeField()
    ipaddr = StringField()
    db_sid = StringField()
    record_id = StringField()
    cmdb_id = IntField()
    plan_hash_value = LongField()  # TODO
    id = IntField()
    depth: IntField()
    parent_id = IntField()
    operation = StringField()
    operation_display: StringField()
    options: StringField()
    object_node = StringField(null=True)
    object_owner = StringField(null=True)
    object_name = StringField(null=True)
    object_type = StringField(null=True)
    optimizer = StringField(null=True)
    search_columns = IntField()
    position = IntField()
    cost = StringField(null=True)
    cardinality = IntField()
    bytes = IntField()
    other_tag = StringField(null=True)
    partition_start = StringField(null=True)
    partition_stop = StringField(null=True)
    partition_id = StringField(null=True)
    other = StringField(null=True)
    distribution = StringField(null=True)
    cpu_cost = StringField(null=True)
    io_cost = StringField(null=True)
    filter_predicates = StringField(null=True)
    access_predicates = StringField(null=True)
    time = StringField(null=True)

    meta = {
        "collection": "sqlplan"
    }
