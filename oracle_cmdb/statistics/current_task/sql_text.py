# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleStatsSQLText"
]

from typing import Union, Generator

import sqlparse
from mongoengine import StringField, IntField, DateTimeField

from utils.datetime_utils import *
from .base import *
from ..base import OracleBaseStatistics
from ...capture import OracleSQLText


@OracleBaseStatistics.need_collect()
class OracleStatsSQLText(OracleBaseCurrentTaskSchemaStatistics):
    """SQL文本出现时间统计"""

    sql_id = StringField(null=False)
    first_appearance = DateTimeField()
    last_appearance = DateTimeField()
    count = IntField(default=0)
    longer_sql_text = StringField(default="")
    longer_sql_text_prettified = StringField(default="")

    meta = {
        "collection": "oracle_stats_sql_text",
        "indexes": [
            "sql_id"
        ]
    }

    # TODO 默认统计近三个月的数据
    LATEST_MONTHS = 3

    @classmethod
    def generate(
            cls,
            task_record_id: int,
            cmdb_id: Union[int, None],
            **kwargs) -> Generator["OracleStatsSQLText", None, None]:

        ret = OracleSQLText.aggregate(
            {
                "$match": {
                    'cmdb_id': cmdb_id,
                    'create_time': {"$gte": arrow.now().shift(
                        months=-cls.LATEST_MONTHS).datetime}
                }
            },
            {
                "$group": {
                    "_id": {
                        "sql_id": "$sql_id",
                        "schema_name": "$schema_name",
                        "longer_sql_text": "$longer_sql_text"
                    },
                    "first_appearance": {"$min": "$create_time"},
                    "last_appearance": {"$max": "$create_time"},
                    "count": {"$sum": 1}
                }
            }
        )
        for one in ret:
            sql_text_prettified = one["_id"]["longer_sql_text"]
            try:
                # 尝试美化sql文本
                sql_text_prettified = sqlparse.format(
                    sql_text_prettified,
                    reindent_aligned=True
                )
            except:
                pass
            doc = cls(
                sql_id=one["_id"]["sql_id"],
                first_appearance=one["first_appearance"],
                last_appearance=one["last_appearance"],
                count=one["count"],
                longer_sql_text=one["_id"]["longer_sql_text"],
                longer_sql_text_prettified=sql_text_prettified
            )
            cls.post_generated(
                doc=doc,
                cmdb_id=cmdb_id,
                task_record_id=task_record_id,
                schema_name=one["_id"]["schema_name"]
            )
            yield doc
