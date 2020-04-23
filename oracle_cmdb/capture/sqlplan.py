# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "SQLPlan"
]

from typing import NoReturn, List

from mongoengine import StringField, IntField

from .base import TwoDaysSQLCapturingDoc
from .. import const


@TwoDaysSQLCapturingDoc.need_collect()
class SQLPlan(TwoDaysSQLCapturingDoc):
    """sql执行计划"""

    # 带缩进用于展示的执行计划
    operation_display = StringField(null=True)
    # operation_display 加上 options的值
    operation_display_with_options = StringField(null=True)

    sql_id = StringField(null=True)
    plan_hash_value = IntField(null=True)
    the_id = IntField(null=True)
    depth = IntField(null=True)
    parent_id = IntField(null=True)
    operation = StringField(null=True)
    options = StringField(null=True)
    object_node = StringField(null=True)
    object_owner = StringField(null=True)
    object_name = StringField(null=True)
    object_type = StringField(null=True)
    optimizer = StringField(null=True)
    search_columns = StringField(null=True)
    position = IntField(null=True)
    cost = StringField(null=True)
    cardinality = StringField(null=True)
    the_bytes = StringField(null=True)
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
        "collection": "sqlplan",
        "indexes": [
            "sql_id",
            "plan_hash_value",
        ]
    }

    @classmethod
    def simple_capture(cls, **kwargs) -> str:
        sql_id: str = kwargs["sql_id"]
        plan_hash_value: int = kwargs["plan_hash_value"]
        return f"""
SELECT
    p.sql_id,
    p.plan_hash_value,
    p.id as the_id,
    p.depth,
    p.parent_id,
    p.operation,
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
"""

    @classmethod
    def post_captured(cls, **kwargs) -> NoReturn:
        docs: [cls] = kwargs["docs"]

        TwoDaysSQLCapturingDoc.post_captured(**kwargs)
        for doc in docs:
            if doc.cost:
                doc.cost = str(int(doc.cost))
            if doc.position:
                doc.position = str(int(doc.position))
            if doc.cpu_cost:
                doc.cpu_cost = str(int(doc.cpu_cost))
            if doc.the_bytes:
                doc.the_bytes = str(int(doc.the_bytes))
            if doc.cardinality:
                doc.cardinality = str(int(doc.cardinality))
            if doc.operation:
                doc.operation_display = " " * doc.depth + doc.operation
                doc.operation_display_with_options = doc.operation_display
                if doc.options:
                    doc.operation_display_with_options = \
                        doc.operation_display + " " + doc.options
