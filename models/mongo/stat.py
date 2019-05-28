# Author: kk.Fang(fkfkbill@gmail.com)

from mongoengine import IntField, StringField, BooleanField, DictField,\
    DateTimeField, ObjectIdField, FloatField

from .utils import BaseDoc


class SQLStat(BaseDoc):
    _id = ObjectIdField()
    sql_id = StringField()
    etl_date = DateTimeField()
    ip_address = StringField()
    db_sid = StringField()
    record_id = StringField()
    cmdb_id = IntField()
    plan_hash_value = IntField()
    schema = StringField()
    module = StringField()
    cpu_time_total = FloatField()
    cpu_time_delta = FloatField()
    per_cpu_time = FloatField()
    disk_reads_total = FloatField()
    disk_reads_delta = FloatField()
    per_disk_reads = FloatField()
    direct_writes_total = FloatField()
    direct_writes_delta = FloatField()
    per_direct_writes = FloatField()
    elapsed_time_total = FloatField()
    elapsed_time_delta = FloatField()
    per_elapsed_time = FloatField()
    buffer_gets_total = IntField()
    buffer_gets_delta = IntField()
    per_buffer_gets = IntField()
    rows_processed_delta = IntField()
    rows_processed_total = IntField()
    rows_processed_gets = IntField()
    executions_total = IntField()
    executions_delta = IntField()
    per_row_blk = FloatField()

    meta = {
        "collection": "sqlstat"
    }
