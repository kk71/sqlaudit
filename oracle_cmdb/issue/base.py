# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleOnlineIssue"
]

import os.path
from typing import NoReturn, Generator, List

from mongoengine import IntField, StringField

import settings
from rule.rule_jar import *
from utils.log_utils import grouped_count_logger
from rule.cmdb_rule import CMDBRule
from issue.issue import *
from ..task.cmdb_task_stats import *
from ..cmdb import OracleCMDB


class OracleOnlineIssue(OnlineIssue):
    """oracle线上审核问题"""

    task_record_id = IntField(required=True)
    schema_name = StringField(required=True)

    meta = {
        "allow_inheritance": True,
        "collection": "oracle_online_issue",
        "indexes": [
            "task_record_id",
            "schema_name"
        ]
    }

    RELATIVE_IMPORT_TOP_PATH_PREFIX = settings.SETTINGS_FILE_DIR

    PATH_TO_IMPORT = os.path.dirname(__file__)

    ENTRIES = ()

    COLLECTED: ["OracleOnlineIssue"] = []

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
    def pack_rule_ret_to_doc(
            cls,
            the_rule: CMDBRule,
            ret: Generator[dict, None, None]) -> List["OracleOnlineIssue"]:
        """统一将规则的返回包装成文档"""
        docs = []
        for minus_score, output_param in ret:
            doc = cls()
            doc.as_issue_of(
                the_rule,
                output_data=output_param,
                minus_score=minus_score
            )
            docs.append(doc)
        return docs

    @classmethod
    def generate_rule_jar(cls,
                          cmdb_id: int,
                          task_record_id: int = None,
                          **kwargs) -> RuleJar:
        the_jar = super().generate_rule_jar(cmdb_id, task_record_id, **kwargs)

        schema_name: str = kwargs.get("schema_name", None)
        if schema_name is None:
            print(f"{schema_name=} so the stats record of entries "
                  f"and rule unique keys is ignored.")
            return the_jar

        # 保存当前使用的规则唯一标识以及entries，记录schema
        OracleCMDBTaskStatsEntriesAndRules.write_stats(
            task_record_id,
            cls,
            entries=the_jar.entries,
            rule_info=the_jar.bulk_to_dict(
                iter_if=lambda k, v: k in ("max_score",),
                need_unique_key=True
            ),
            schema_name=schema_name
        )
        return the_jar

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
                    print(f"... in {schema_name}")
                    docs += list(m.simple_analyse(
                        task_record_id=task_record_id,
                        schema_name=schema_name,
                        cmdb_id=cmdb_id
                    ))
                if docs:
                    cls.objects.insert(docs)
                counter(m.__doc__, len(docs))
