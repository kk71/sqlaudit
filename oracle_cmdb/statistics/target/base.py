# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleBaseTargetCMDBStatistics",
    "OracleBaseTargetSchemaStatistics"
]

from mongoengine import IntField, StringField

from ...cmdb import *
from ..base import OracleBaseStatistics


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
    def cmdb_latest_available_task_record_id_for_stats(
            cls,
            cmdb_id: int,
            task_record_id: int,
            target_cmdb: OracleCMDB,
            **kwargs) -> int:
        """
        判断当前处理的纳管库的最后一次可供统计用的task_record_id应该取哪个
        :param cmdb_id: 当前任务发起的cmdb_id
        :param task_record_id: 当前任务的task_record_id
        :param target_cmdb: 当前想要统计的纳管库的cmdb对象
        :param kwargs:
        :return:
        """
        from ...tasks.capture.cmdb_task_capture import OracleCMDBTaskCapture

        if cmdb_id == target_cmdb.cmdb_id:
            # 默认如果正在统计的纳管库就是当前库，
            # 那么就取当前任务的task_record_id最为当前库的最后可用task_record_id
            return task_record_id
        else:
            return OracleCMDBTaskCapture.get_cmdb_task_by_cmdb(
                target_cmdb).last_success_task_record_id


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

