# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleBaseCurrentTaskCMDBStatistics",
    "OracleBaseCurrentTaskSchemaStatistics"
]

from typing import List

from mongoengine import StringField

from ..base import OracleBaseStatistics


class OracleBaseCurrentTaskCMDBStatistics(OracleBaseStatistics):
    """当前任务库的统计"""

    meta = {
        "abstract": True
    }


class OracleBaseCurrentTaskSchemaStatistics(OracleBaseCurrentTaskCMDBStatistics):
    """当前任务库的schema的统计"""

    schema_name = StringField(required=True, null=True)

    meta = {
        "abstract": True,
        "indexes": [
            "schema_name"
        ]
    }

    @classmethod
    def post_generated(cls, **kwargs):
        super().post_generated(**kwargs)
        schema_name: str = kwargs["schema_name"]
        docs: List["OracleBaseCurrentTaskSchemaStatistics"] = kwargs["docs"]

        for doc in docs:
            doc.schema_name = schema_name

