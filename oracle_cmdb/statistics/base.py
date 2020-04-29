# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleBaseStatistics",
    "OracleBaseCMDBStatistics",
    "OracleBaseSchemaStatistics",
    "OracleBaseLoginUserStatistics"
]

import os.path

from mongoengine import IntField, StringField

import settings
import core.statistics
from models.mongoengine import *


class OracleBaseStatistics(
        BaseDoc,
        core.statistics.BaseStatisticItem,
        metaclass=SelfCollectingTopLevelDocumentMetaclass):
    """基础oracle统计"""

    # TODO 注意，这个任务记录id和纳管库id只是说当前的统计数据是哪次任务触发的
    # TODO 并不代表当前的统计数据是这个库的
    # TODO 因此尽量不要直接使用本基类，而是使用下面具体细化的基类进行编码
    task_record_id = IntField(required=True)
    cmdb_id = IntField(required=True)

    meta = {
        "abstract": True,
        "indexes": [
            "task_record_id",
            "cmdb_id"
        ]
    }

    COLLECTED: ["OracleBaseStatistics"] = []

    RELATIVE_IMPORT_TOP_PATH_PREFIX = settings.SETTINGS_FILE_DIR

    PATH_TO_IMPORT = os.path.dirname(__file__)

    @classmethod
    def post_generated(cls, **kwargs):
        docs: ["OracleBaseStatistics"] = kwargs["docs"]
        task_record_id: int = kwargs["task_record_id"]
        cmdb_id: int = kwargs["cmdb_id"]

        for doc in docs:
            doc.task_record_id = task_record_id
            doc.cmdb_id = cmdb_id

    @classmethod
    def process(cls, collected=None, **kwargs):
        return


class OracleBaseCMDBStatistics(OracleBaseStatistics):
    """纳管库级别的统计"""

    target_cmdb_id = IntField(required=True, null=True)

    meta = {
        "abstract": True,
        "indexes": [
            "target_cmdb_id"
        ]
    }

    @classmethod
    def post_generated(cls, **kwargs):
        super().post_generated(**kwargs)
        target_cmdb_id: int = kwargs["target_cmdb_id"]
        docs: ["OracleBaseCMDBStatistics"] = kwargs["docs"]

        for doc in docs:
            doc.target_cmdb_id = target_cmdb_id


class OracleBaseSchemaStatistics(OracleBaseCMDBStatistics):
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
        docs: ["OracleBaseSchemaStatistics"] = kwargs["docs"]

        for doc in docs:
            doc.target_schema_name = target_schema_name


class OracleBaseLoginUserStatistics(OracleBaseStatistics):
    """登录用户级别的统计"""

    target_login_user = StringField(required=True, null=True)

    meta = {
        "abstract": True,
        "indexes": [
            "target_login_user"
        ]
    }

    @classmethod
    def post_generated(cls, **kwargs):
        super().post_generated(**kwargs)
        target_login_user: str = kwargs["target_login_user"]
        docs: ["OracleBaseLoginUserStatistics"] = kwargs["docs"]

        for doc in docs:
            doc.target_login_user = target_login_user

