# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleStatsSQLPLan"
]

from typing import Union, Generator

from mongoengine import StringField, IntField, DateTimeField

from utils.datetime_utils import *
from ... import const
from .base import *
from ..base import OracleBaseStatistics
from ...capture import OracleSQLPlan


# TODO 写完了才发现这个统计数据似乎没啥用？暂时先不启用吧
# @OracleBaseStatistics.need_collect()
class OracleStatsSQLPLan(OracleBaseCurrentTaskSchemaStatistics):
    """执行计划出现时间统计"""

    sql_id = StringField(null=False)
    plan_hash_value = IntField(null=False)
    first_appearance = DateTimeField()
    last_appearance = DateTimeField()

    meta = {
        "collection": "oracle_stats_sql_plan",
        "indexes": [
            "sql_id",
            ("sql_id", "plan_hash_value")
        ]
    }

    # TODO 默认统计近三个月的数据
    LATEST_MONTHS = 3

    @classmethod
    def generate(
            cls,
            task_record_id: int,
            cmdb_id: Union[int, None],
            **kwargs) -> Generator["OracleStatsSQLPLan", None, None]:

        ret = OracleSQLPlan.aggregate(
            {
                "$match": {
                    "two_days_capture": const.SQL_TWO_DAYS_CAPTURE_TODAY,
                    'cmdb_id': cmdb_id,
                    'create_time': {"$gte": arrow.now().shift(
                        months=-cls.LATEST_MONTHS).datetime}
                }
            },
            {
                "$group": {
                    "_id": {
                        "sql_id": "$sql_id",
                        "plan_hash_value": "$plan_hash_value",
                        "schema_name": "$schema_name"
                    },
                    "first_appearance": {"$min": "$create_time"},
                    "last_appearance": {"$max": "$create_time"},
                }
            }
        )
        for one in ret:
            doc = cls(
                sql_id=one["_id"]["sql_id"],
                plan_hash_value=one["_id"]["plan_hash_value"],
                first_appearance=one["first_appearance"],
                last_appearance=one["last_appearance"]
            )
            cls.post_generated(
                doc=doc,
                cmdb_id=cmdb_id,
                task_record_id=task_record_id,
                schema_name=one["_id"]["schema_name"]
            )
            yield doc
