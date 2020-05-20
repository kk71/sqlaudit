# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleObjectCapturingDoc",
    "OracleSchemaObjectCapturingDoc"
]

from typing import NoReturn

from mongoengine import ObjectIdField, IntField, StringField

from .base import *
from utils.log_utils import *
from models.mongoengine import *
from oracle_cmdb.plain_db import OraclePlainConnector


class OracleObjectCapturingDoc(
        BaseDoc,
        BaseOracleCapture,
        metaclass=SelfCollectingTopLevelDocumentMetaclass):
    """对象采集"""

    _id = ObjectIdField()
    cmdb_id = IntField()
    task_record_id = IntField(required=True)

    meta = {
        'abstract': True,
        "indexes": [
            "cmdb_id",
            "task_record_id"
        ]
    }

    COLLECTED = []

    @classmethod
    def post_captured(cls, **kwargs) -> NoReturn:
        docs: ["OracleObjectCapturingDoc"] = kwargs["docs"]
        cmdb_id: int = kwargs["cmdb_id"]
        task_record_id: int = kwargs["task_record_id"]

        for d in docs:
            d.from_dict({
                "cmdb_id": cmdb_id,
                "task_record_id": task_record_id,
            })

    @classmethod
    def process(cls, collected: ["OracleObjectCapturingDoc"] = None, **kwargs):
        if collected is None:
            collected = cls.COLLECTED
        cmdb_id: int = kwargs["cmdb_id"]
        task_record_id: int = kwargs["task_record_id"]
        cmdb_conn: OraclePlainConnector = kwargs["cmdb_connector"]

        for i, m in enumerate(collected):
            i += 1
            total = len(collected)
            print(f"* running {i} of {total}: {m.__doc__}")
            sql_to_run = m.simple_capture()
            docs = [
                m(**c)
                for c in cmdb_conn.select_dict(sql_to_run)]
            if not docs:
                print("no data captured.")
                continue
            m.post_captured(
                docs=docs,
                cmdb_id=cmdb_id,
                task_record_id=task_record_id
            )
            docs_inserted = m.objects.insert(docs)
            print(f"{len(docs_inserted)} captured.")


class OracleSchemaObjectCapturingDoc(OracleObjectCapturingDoc):
    """面向schema的对象采集"""

    schema_name = StringField(required=True)

    meta = {
        'abstract': True,
        "indexes": [
            "schema_name"
        ]
    }

    COLLECTED = []

    @classmethod
    def post_captured(cls, **kwargs) -> NoReturn:
        docs: ["OracleSchemaObjectCapturingDoc"] = kwargs["docs"]
        cmdb_id: int = kwargs["cmdb_id"]
        task_record_id: int = kwargs["task_record_id"]
        obj_owner: str = kwargs["obj_owner"]

        for d in docs:
            d.from_dict({
                "cmdb_id": cmdb_id,
                "schema_name": obj_owner,
                "task_record_id": task_record_id,
            })

    @classmethod
    def process(cls, collected: ["OracleSchemaObjectCapturingDoc"] = None, **kwargs):
        if collected is None:
            collected = cls.COLLECTED
        cmdb_id: int = kwargs["cmdb_id"]
        task_record_id: int = kwargs["task_record_id"]
        cmdb_conn: OraclePlainConnector = kwargs["cmdb_connector"]
        schemas: [str] = kwargs["schemas"]

        for i, m in enumerate(collected):
            i += 1
            total = len(collected)
            print(f"* running {i} of {total}: {m.__doc__}")
            with grouped_count_logger(
                    m.__doc__, item_type_name="schema") as schema_counter:
                for a_schema in schemas:
                    sql_to_run = m.simple_capture(obj_owner=a_schema)
                    docs = [
                        m(**c)
                        for c in cmdb_conn.select_dict(sql_to_run)]
                    if not docs:
                        schema_counter(a_schema, 0)
                        continue
                    m.post_captured(
                        cmdb_id=cmdb_id,
                        task_record_id=task_record_id,
                        obj_owner=a_schema,
                        docs=docs,
                        cmdb_connector=cmdb_conn
                    )
                    docs_inserted = m.objects.insert(docs)
                    schema_counter(a_schema, len(docs_inserted))
