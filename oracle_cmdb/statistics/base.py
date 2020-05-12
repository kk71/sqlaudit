# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleBaseStatistics"
]

import os.path
from typing import List, Generator

from mongoengine import IntField

import settings
import core.statistics
from auth.user import User
from utils.log_utils import *
from models.mongoengine import *
from ..issue import OracleOnlineIssue
from ..cmdb import *


class OracleBaseStatistics(
        BaseDoc,
        core.statistics.BaseStatisticItem,
        metaclass=SelfCollectingTopLevelDocumentMetaclass):
    """基础oracle统计"""

    # TODO 注意，这个任务记录id和纳管库id只是说当前的统计数据是哪次任务触发的
    # TODO 并不代表当前的统计数据是这个库的
    # TODO 因此不要直接使用本基类，而是使用下面具体细化的基类进行编码
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

    # 需要作统计的issue models
    ISSUES: tuple = (OracleOnlineIssue,)

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
            collected = cls.SORTED_COLLECTED_BY_REQUIREMENT
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

    @classmethod
    def issue_entries(cls) -> tuple:
        """返回ISSUES内所有issue的models的entries合集"""
        ret = {j for i in cls.ISSUES for j in i.ENTRIES}
        return tuple(ret)

    @classmethod
    def users(cls, session, **kwargs) -> Generator[User, None, None]:
        """获取login_user列表"""
        yield from session.query(User)

    @classmethod
    def cmdbs(cls, session, **kwargs) -> Generator[OracleCMDB, None, None]:
        yield from session.query(OracleCMDB)

    @classmethod
    def schemas(cls, session, cmdb_id: int, **kwargs) -> Generator[str, None, None]:
        the_cmdb = session.query(OracleCMDB).filter_by(
            cmdb_id=cmdb_id).first()
        yield from the_cmdb.get_bound_schemas()

