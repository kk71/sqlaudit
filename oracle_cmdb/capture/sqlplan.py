# Author: kk.Fang(fkfkbill@gmail.com)

from mongoengine import StringField, IntField, FloatField, DateField

from ..plain_db import *
from .base import TwoDaysSQLCapturingDoc


class SQLPlan(TwoDaysSQLCapturingDoc):
    """纳管库sql执行计划"""

    operation_display = StringField()  # 带缩进用于展示的执行计划
    operation_display_with_options = StringField()  # operation_display 加上 options的值

    sql_id = StringField()
    plan_hash_value = IntField()
    the_id = IntField()
    depth = IntField()
    parent_id = IntField()
    operation = StringField()
    options = StringField()
    object_node = StringField()
    object_owner = StringField()
    object_name = StringField()
    object_type = StringField()
    optimizer = StringField()
    search_columns = StringField()
    position = IntField()
    cost = StringField()
    cardinality = StringField()
    the_bytes = StringField()
    other_tag = StringField()
    partition_start = StringField()
    partition_stop = StringField()
    partition_id = StringField()
    other = StringField()
    distribution = StringField()
    cpu_cost = StringField()
    io_cost = StringField()
    filter_predicates = StringField()
    access_predicates = StringField()
    time = StringField()

    @classmethod
    def capture_sql(cls,
                    cmdb_connector: OraclePlainConnector,
                    schema_name: str,
                    sql_id: str,
                    **kwargs):
        plan_hash_value: int = kwargs["plan_hash_value"]
        records = cmdb_connector.select_dict(f"""
SELECT
    p.sql_id,
    p.plan_hash_value,
    p.id as the_id,
    p.depth,
    p.parent_id,
    p.operation,
    // lpad(' ', p.depth) || p.operation operation_display,
    p.options,
    p.object_node,
    p.object_owner,
    p.object_name,
    p.object_type,
    p.optimizer,
    p.search_columns,
    p.position,
    p.cost,
    p.cardinality,
    p.bytes as the_bytes,
    p.other_tag,
    p.partition_start,
    p.partition_stop,
    p.partition_id,
    p.other,
    p.distribution,
    p.cpu_cost,
    p.io_cost,
    p.filter_predicates,
    p.access_predicates,
    p.time
FROM
    (
        SELECT * FROM
            (
                SELECT
                    sql_id,
                    plan_hash_value,
                    id,
                    depth,
                    parent_id,
                    operation,
                    lpad(' ', 2 * depth)
                    || operation operation_display,
                    options,
                    object_node,
                    object_owner,
                    object_name,
                    object_type,
                    optimizer,
                    search_columns,
                    position,
                    cost,
                    cardinality,
                    bytes,
                    other_tag,
                    partition_start,
                    partition_stop,
                    partition_id,
                    other,
                    distribution,
                    cpu_cost,
                    io_cost,
                    filter_predicates,
                    access_predicates,
                    time,
                    ROW_NUMBER() OVER(
                        PARTITION BY sql_id, plan_hash_value,id
                        ORDER BY
                            timestamp DESC
                    ) rn
                FROM
                    dba_hist_sql_plan
                WHERE
                    sql_id = '{sql_id}'
                    AND plan_hash_value = {plan_hash_value}
                    AND id <= 799
            )
        WHERE
            rn = 1
    ) p
""")
        for record in records:
            new_doc = cls(

                **record
            )
            for i in range(len(value)):
                temp_dict.update({
                    columns[i]: value[i],
                    'USERNAME': username,
                    'ETL_DATE': self.capture_time,
                    'IPADDR': self.ipaddress,
                    'DB_SID': self.sid,
                    'record_id': "##".join([str(self.record_id), username]),
                    'cmdb_id': self.cmdb_id,
                })
            if temp_dict.get("COST") == 18446744073709551615:
                temp_dict["COST"] = str(int(temp_dict["COST"]))
            if temp_dict.get("POSITION", None) == 18446744073709551615:
                temp_dict["POSITION"] = str(int(temp_dict["POSITION"]))
            if temp_dict.get("CPU_COST", None) == 18446744073709551615:
                temp_dict["CPU_COST"] = str(int(temp_dict["CPU_COST"]))
            if temp_dict.get("BYTES", None) == 18446744073709551615:
                temp_dict["BYTES"] = str(int(temp_dict["BYTES"]))
            if temp_dict.get("CARDINALITY", None) == 18446744073709551615:
                temp_dict["CARDINALITY"] = str(int(temp_dict["CARDINALITY"]))
