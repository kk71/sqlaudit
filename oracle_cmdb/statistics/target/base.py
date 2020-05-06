# Author: kk.Fang(fkfkbill@gmail.com)

from typing import Generator

from mongoengine import IntField, StringField

from ...cmdb import *
from oracle_cmdb.statistics import OracleBaseStatistics


class OracleBaseTargetCMDBStatistics(OracleBaseStatistics):
    """纳管库级别的统计"""

    target_cmdb_id = IntField(required=True, null=True)
    target_task_record_id = IntField(required=True, null=True)

    meta = {
        "abstract": True,
        "indexes": [
            "target_cmdb_id",
            "target_task_record_id",
        ]
    }

    @classmethod
    def post_generated(cls, **kwargs):
        super().post_generated(**kwargs)
        target_cmdb_id: int = kwargs["target_cmdb_id"]
        target_task_record_id: int = kwargs["target_task_record_id"]
        doc: "OracleBaseTargetCMDBStatistics" = kwargs["doc"]

        doc.target_cmdb_id = target_cmdb_id
        doc.target_task_record_id = target_task_record_id

    @classmethod
    def cmdbs(cls, session, **kwargs) -> Generator[OracleCMDB, None, None]:
        yield from session.query(OracleCMDB)


class OracleBaseTargetSchemaStatistics(OracleBaseTargetCMDBStatistics):
    """纳管库schema级别的统计"""

    target_schema_name = StringField(required=True, null=True)

    meta = {
        "abstract": True,
        "indexes": [
            "target_schema_name"
        ]
    }

    @classmethod
    def post_generated(cls, **kwargs):
        super().post_generated(**kwargs)
        target_schema_name: str = kwargs["target_schema_name"]
        doc: "OracleBaseTargetSchemaStatistics" = kwargs["doc"]

        doc.target_schema_name = target_schema_name

    @classmethod
    def schemas(cls, session, cmdb_id: int, **kwargs) -> Generator[str, None, None]:
        the_cmdb = session.query(OracleCMDB).filter_by(
                cmdb_id=cmdb_id).first()
        yield from the_cmdb.get_bound_schemas()
