# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "SQLStat"
]

from mongoengine import StringField, IntField, FloatField

from .base import TwoDaysSQLCapturingDoc


@TwoDaysSQLCapturingDoc.need_collect()
class SQLStat(TwoDaysSQLCapturingDoc):
    """纳管库sql执行信息"""

    sql_id = StringField(null=True)
    plan_hash_value = IntField(null=True)
    parsing_schema_name = StringField(null=True)
    module = StringField(null=True)
    cpu_time_total = FloatField(null=True)
    cpu_time_delta = FloatField(null=True)
    per_cpu_time = FloatField(null=True)
    disk_reads_total = FloatField(null=True)
    disk_reads_delta = FloatField(null=True)
    per_disk_reads = FloatField(null=True)
    direct_writes_total = FloatField(null=True)
    direct_writes_delta = FloatField(null=True)
    per_direct_writes = FloatField(null=True)
    elapsed_time_total = FloatField(null=True)
    elapsed_time_delta = FloatField(null=True)
    per_elapsed_time = FloatField(null=True)
    buffer_gets_total = FloatField(null=True)
    buffer_gets_delta = FloatField(null=True)
    per_buffer_gets = FloatField(null=True)
    rows_processed_delta = IntField(null=True)
    rows_processed_total = IntField(null=True)
    rows_processed_gets = IntField(null=True)
    executions_total = IntField(null=True)
    executions_delta = IntField(null=True)
    per_row_blk = FloatField(null=True)

    meta = {
        "collection": "sqlstat",
        "indexes": [
            "sql_id",
            "plan_hash_value",
        ]
    }

    @classmethod
    def simple_capture(cls, **kwargs) -> str:
        sql_id: str = kwargs["sql_id"]
        plan_hash_value: int = kwargs["plan_hash_value"]
        schema_name: str = kwargs["schema_name"]
        snap_id_s: int = kwargs["snap_id_s"]
        snap_id_e: int = kwargs["snap_id_e"]

        return f"""
SELECT t.sql_id,
       t.plan_hash_value,
       t.parsing_schema_name,
       t.module,
       round(sum(t.cpu_time_total) / 1000000, 4) AS cpu_time_total,
       round(sum(t.cpu_time_delta) / 1000000, 4) AS cpu_time_delta,
       round(ceil((sum(cpu_time_delta) / decode(sum(executions_delta), 0, 1, sum(executions_delta)))) / 1000000, 4) per_cpu_time,
       sum(t.disk_reads_total) AS disk_reads_total,
       sum(t.disk_reads_delta) AS disk_reads_delta,
       ceil((sum(disk_reads_delta) / decode(sum(executions_delta), 0, 1, sum(executions_delta)))) per_disk_reads,
       sum(t.direct_writes_total) AS direct_writes_total,
       sum(t.direct_writes_delta) AS direct_writes_delta,
       ceil((sum(direct_writes_delta) / decode(sum(executions_delta), 0, 1, sum(executions_delta)))) per_direct_writes,
       round(sum(t.elapsed_time_total) / 1000000, 4) AS elapsed_time_total,
       round(sum(t.elapsed_time_delta) / 1000000, 4) AS elapsed_time_delta,
       round(ceil((sum(elapsed_time_delta) / decode(sum(executions_delta), 0, 1, sum(executions_delta)))) / 1000000, 4) per_elapsed_time,
       sum(t.buffer_gets_total) AS buffer_gets_total,
       sum(t.buffer_gets_delta) AS buffer_gets_delta,
       ceil((sum(buffer_gets_delta) / decode(sum(executions_delta), 0, 1, sum(executions_delta)))) per_buffer_gets,
       sum(t.rows_processed_delta) AS rows_processed_delta,
       sum(t.rows_processed_total) AS rows_processed_total,
       ceil((sum(rows_processed_delta) / decode(sum(executions_delta), 0, 1, sum(executions_delta)))) rows_processed_gets,
       sum(t.executions_total) AS executions_total,
       sum(t.executions_delta) AS executions_delta,
       round(sum(DISK_READS_DELTA) + sum(BUFFER_GETS_DELTA) / decode(sum(ROWS_PROCESSED_DELTA), 0, 1, sum(ROWS_PROCESSED_DELTA)), 3) AS per_row_blk
FROM dba_hist_sqlstat t
WHERE t.snap_id BETWEEN '{snap_id_s}' AND '{snap_id_e}'
  AND t.parsing_schema_name = '{schema_name}'
  AND t.sql_id = '{sql_id}'
  AND t.plan_hash_value = '{plan_hash_value}'
GROUP BY sql_id,
         plan_hash_value,
         t.parsing_schema_name,t.module
"""

