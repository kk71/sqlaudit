# Author: kk.Fang(fkfkbill@gmail.com)

from mongoengine import IntField, StringField, BooleanField, DictField,\
    DateTimeField, ObjectIdField, FloatField

from .utils import BaseDoc


class SQLStat(BaseDoc):
    _id = ObjectIdField()
    sql_id = StringField("SQL_ID")
    etl_date = DateTimeField("ETL_DATE")
    ip_address = StringField("IPADDR")
    db_sid = StringField("DB_SID")
    record_id = StringField()
    cmdb_id = IntField()
    plan_hash_value = IntField("PLAN_HASH_VALUE")
    schema = StringField("USERNAME")
    module = StringField("MODULE")
    cpu_time_total = FloatField("CPU_TIME_TOTAL")
    cpu_time_delta = FloatField("CPU_TIME_DELTA")
    per_cpu_time = FloatField("PER_CPU_TIME")
    disk_reads_total = FloatField("DISK_READS_TOTAL")
    disk_reads_delta = FloatField("DISK_READS_DELTA")
    per_disk_reads = FloatField("PER_DISK_READS")
    direct_writes_total = FloatField("DIRECT_WRITES_TOTAL")
    direct_writes_delta = FloatField("DIRECT_WRITES_DELTA")
    per_direct_writes = FloatField("PER_DIRECT_WRITES")
    elapsed_time_total = FloatField("ELAPSED_TIME_TOTAL")
    elapsed_time_delta = FloatField("ELAPSED_TIME_DELTA")
    per_elapsed_time = FloatField("PER_ELAPSED_TIME")
    buffer_gets_total = IntField("BUFFER_GETS_TOTAL")
    buffer_gets_delta = IntField("BUFFER_GETS_DELTA")
    per_buffer_gets = IntField("PER_BUFFER_GETS")
    rows_processed_delta = IntField("ROWS_PROCESSED_DELTA")
    rows_processed_total = IntField("ROWS_PROCESSED_TOTAL")
    rows_processed_gets = IntField("ROWS_PROCESSED_GETS")
    executions_total = IntField("EXECUTIONS_TOTAL")
    executions_delta = IntField("EXECUTIONS_DELTA")
    per_row_blk = FloatField("PER_ROW_BLK")

    meta = {
        "collection": "sqlstat"
    }
