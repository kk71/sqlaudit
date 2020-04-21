# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleOnlineIssue"
]

import abc
import os.path
from typing import NoReturn, Generator

from mongoengine import IntField, StringField

import settings
import cmdb.const
from utils.log_utils import grouped_count_logger
from rule.rule import CMDBRule
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
    @abc.abstractmethod
    def _single_rule_analyse(
            cls,
            the_rule: CMDBRule,
            entries: [str],
            the_cmdb: OracleCMDB,
            task_record_id: int,
            schema_name: str,
            **kwargs) -> Generator["OracleOnlineIssue", None, None]:
        """单个schema的sqls以单个规则分析"""
        pass

    @classmethod
    def pack_rule_ret_to_doc(
            cls,
            the_rule: CMDBRule,
            ret: list) -> Generator["OracleOnlineIssue", None, None]:
        """统一将规则的返回包装成文档"""
        for minus_score, output_param in ret:
            doc = cls()
            doc.as_issue_of(
                the_rule,
                output_data=output_param,
                minus_score=minus_score
            )
            yield doc

    @classmethod
    def process(cls, collected=None, **kwargs):
        task_record_id: int = kwargs["task_record_id"]
        cmdb_id: int = kwargs["cmdb_id"]
        schemas: [str] = kwargs["schemas"]

        if collected is None:
            collected = cls.COLLECTED
        with grouped_count_logger(cls.__doc__, item_type_name="维度") as counter:
            for i, m in enumerate(collected):
                i += 1
                total = len(collected)
                print(f"* running {i} of {total}: {m.__doc__}")
                docs = []
                for schema_name in schemas:
                    docs += list(m.simple_analyse(
                        task_record_id=task_record_id,
                        schema_name=schema_name,
                        cmdb_id=cmdb_id
                    ))
                    cls.post_analysed(
                        docs=docs,
                        task_record_id=task_record_id,
                        schema_name=schema_name
                    )
                if docs:
                    cls.objects.insert(docs)
                counter(m.__doc__, len(docs))
