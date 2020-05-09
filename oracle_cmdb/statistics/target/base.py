# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleBaseTargetCMDBStatistics",
    "OracleBaseTargetSchemaStatistics"
]

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

    @classmethod
    def cmdb_latest_available_task_record_id_for_stats(
            cls,
            session,
            cmdb_id: int,
            task_record_id: int,
            target_cmdb: OracleCMDB,
            **kwargs) -> int:
        """
        判断当前处理的纳管库的最后一次可供统计用的task_record_id应该取哪个
        :param session:
        :param cmdb_id: 当前任务发起的cmdb_id
        :param task_record_id: 当前任务的task_record_id
        :param target_cmdb: 当前想要统计的纳管库的cmdb对象
        :param kwargs:
        :return:
        """
        if cmdb_id == target_cmdb.cmdb_id:
            # 默认如果正在统计的纳管库就是当前库，
            # 那么就取当前任务的task_record_id最为当前库的最后可用task_record_id
            return task_record_id
        else:
            return target_cmdb.cmdb_task(session).last_success_task_record_id


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
