# Author: kk.Fang(fkfkbill@gmail.com)

from mongoengine import IntField, StringField, BooleanField, DictField,\
    DateTimeField, ObjectIdField, FloatField

from .utils import BaseDoc


class SQLStat(BaseDoc):
    _id = ObjectIdField()
    sql_id = StringField(db_field="SQL_ID")
    etl_date = DateTimeField(db_field="ETL_DATE")
    ip_address = StringField(db_field="IPADDR")
    db_sid = StringField(db_field="DB_SID")
    record_id = StringField()
    cmdb_id = IntField()
    plan_hash_value = IntField(db_field="PLAN_HASH_VALUE")
    schema = StringField(db_field="USERNAME")
    module = StringField(db_field="MODULE")
    cpu_time_total = FloatField(db_field="CPU_TIME_TOTAL")
    cpu_time_delta = FloatField(db_field="CPU_TIME_DELTA")
    per_cpu_time = FloatField(db_field="PER_CPU_TIME")
    disk_reads_total = FloatField(db_field="DISK_READS_TOTAL")
    disk_reads_delta = FloatField(db_field="DISK_READS_DELTA")
    per_disk_reads = FloatField(db_field="PER_DISK_READS")
    direct_writes_total = FloatField(db_field="DIRECT_WRITES_TOTAL")
    direct_writes_delta = FloatField(db_field="DIRECT_WRITES_DELTA")
    per_direct_writes = FloatField(db_field="PER_DIRECT_WRITES")
    elapsed_time_total = FloatField(db_field="ELAPSED_TIME_TOTAL")
    elapsed_time_delta = FloatField(db_field="ELAPSED_TIME_DELTA")  # in ms
    per_elapsed_time = FloatField(db_field="PER_ELAPSED_TIME")
    buffer_gets_total = IntField(db_field="BUFFER_GETS_TOTAL")
    buffer_gets_delta = IntField(db_field="BUFFER_GETS_DELTA")
    per_buffer_gets = IntField(db_field="PER_BUFFER_GETS")
    rows_processed_delta = IntField(db_field="ROWS_PROCESSED_DELTA")
    rows_processed_total = IntField(db_field="ROWS_PROCESSED_TOTAL")
    rows_processed_gets = IntField(db_field="ROWS_PROCESSED_GETS")
    executions_total = IntField(db_field="EXECUTIONS_TOTAL")
    executions_delta = IntField(db_field="EXECUTIONS_DELTA")
    per_row_blk = FloatField(db_field="PER_ROW_BLK")

    meta = {
        "collection": "sqlstat"
    }
