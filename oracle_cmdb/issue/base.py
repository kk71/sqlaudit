# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleOnlineIssue"
]

import os.path
from typing import NoReturn, Generator

from mongoengine import IntField, StringField

import settings
import cmdb.const
from rule.rule_jar import *
from issue.issue import *
from ..cmdb import OracleCMDB


class OracleOnlineIssue(OnlineIssue):
    """oracle线上审核问题"""

    task_record_id = IntField(required=True)
    schema_name = StringField(required=True)

    meta = {
        "allow_inheritance": True,
        "collection": "oracle_online_issue",
        "index": [
            "task_record_id",
            "schema_name"
        ]
    }

    RELATIVE_IMPORT_TOP_PATH_PREFIX = settings.SETTINGS_FILE_DIR

    PATH_TO_IMPORT = os.path.dirname(__file__)

    ENTRIES = ()

    COLLECTED: ["OracleOnlineIssue"] = []

    @classmethod
    def generate_rule_jar(cls, **kwargs) -> RuleJar:
        return super().generate_rule_jar(
            db_type=cmdb.const.DB_ORACLE, **kwargs)

    @staticmethod
    def build_connector(cmdb_id: int):
        return OracleCMDB.build_connector_by_cmdb_id(cmdb_id=cmdb_id)

    @classmethod
    def post_analysed(cls, **kwargs) -> NoReturn:
        docs: ["OracleOnlineIssue"] = kwargs["docs"]
        task_record_id: int = kwargs["task_record_id"]
        schema_name: str = kwargs["schema_name"]

        for doc in docs:
            doc.task_record_id = task_record_id
            doc.schema_name = schema_name

    @classmethod
    def _schema_rule_analyse(
            cls,
            schema_name: str,
            **kwargs) -> Generator["OracleOnlineIssue"]:
        pass

    @classmethod
    def process(cls, collected=None, **kwargs):
        task_record_id: int = kwargs["task_record_id"]
        schemas: [str] = kwargs["schemas"]

        if collected is None:
            collected = cls.COLLECTED
