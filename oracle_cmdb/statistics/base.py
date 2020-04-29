# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleBaseStatistics",
    "OracleBaseTargetLoginUserStatistics"
]

import os.path
from typing import List

from mongoengine import IntField, StringField

import settings
import core.statistics
from utils.log_utils import *
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
        doc: "OracleBaseStatistics" = kwargs["doc"]
        task_record_id: int = kwargs["task_record_id"]
        cmdb_id: int = kwargs["cmdb_id"]

        doc.task_record_id = task_record_id
        doc.cmdb_id = cmdb_id

    @classmethod
    def process(cls, collected: List["OracleBaseStatistics"] = None, **kwargs):
        assert "cmdb_id" in kwargs.keys()
        assert "task_record_id" in kwargs.keys()
        assert "schemas" in kwargs.keys()
        if collected is None:
            collected = cls.COLLECTED
        with grouped_count_logger(
                cls.__doc__, item_type_name="统计") as counter:
            for i, m in enumerate(collected):
                i += 1
                total = len(collected)
                print(f"* running {i} of {total}: {m.__doc__}")
                docs = list(m.generate(**kwargs))
                if docs:
                    m.objects.insert(docs)
                counter(m.__doc__, len(docs))


class OracleBaseTargetLoginUserStatistics(OracleBaseStatistics):
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
        doc: "OracleBaseTargetLoginUserStatistics" = kwargs["doc"]

        doc.target_login_user = target_login_user

    @classmethod
    def login_users(cls) -> List[str]:
        """获取login_user列表"""
        return []
