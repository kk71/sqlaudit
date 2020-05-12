# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleBaseCurrentCMDBStatistics",
    "OracleBaseCurrentCMDBSchemaStatistics"
]

from mongoengine import StringField

from ..base import *


class OracleBaseCurrentCMDBStatistics(OracleBaseStatistics):
    """当前纳管库的统计"""

    meta = {
        "abstract": True
    }


class OracleBaseCurrentCMDBSchemaStatistics(OracleBaseStatistics):
    """当前纳管库schema的统计"""

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
        doc: "OracleBaseCurrentCMDBSchemaStatistics" = kwargs["doc"]

        doc.schema_name = schema_name

