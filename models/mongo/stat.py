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
    cpu_time_total = IntField()
    cpu_time_delta = IntField()
    per_cpu_time = IntField()
    disk_reads_total = IntField()
    disk_reads_delta = IntField()
    per_disk_reads = IntField()
    direct_writes_total = IntField()
    direct_writes_delta = IntField()
    per_direct_writes = IntField()
    elapsed_time_total = IntField()
    elapsed_time_delta = IntField()
    per_elapsed_time = IntField()
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
