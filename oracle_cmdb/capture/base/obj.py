# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "ObjectCapturingDoc",
    "SchemaObjectCapturingDoc"
]

from typing import NoReturn

from mongoengine import ObjectIdField, IntField, StringField

import core.capture
from utils.log_utils import *
from models.mongoengine import BaseDoc, ABCTopLevelDocumentMetaclass
from oracle_cmdb.plain_db import OraclePlainConnector


class ObjectCapturingDoc(
        BaseDoc,
        core.capture.BaseCaptureItem,
        metaclass=ABCTopLevelDocumentMetaclass):
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

    MODELS = []

    @classmethod
    def post_captured(cls, **kwargs) -> NoReturn:
        docs: ["ObjectCapturingDoc"] = kwargs["docs"]
        cmdb_id: int = kwargs["cmdb_id"]
        task_record_id: int = kwargs["task_record_id"]

        for d in docs:
            d.from_dict({
                "cmdb_id": cmdb_id,
                "task_record_id": task_record_id,
            })

    @classmethod
    def capture(cls, model_to_capture: ["ObjectCapturingDoc"] = None, **kwargs):
        if model_to_capture is None:
            model_to_capture = cls.MODELS
        cmdb_id: int = kwargs["cmdb_id"]
        task_record_id: int = kwargs["task_record_id"]
        cmdb_conn: OraclePlainConnector = kwargs["cmdb_connector"]

        for i, m in enumerate(model_to_capture):
            i += 1
            total = len(model_to_capture)
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


class SchemaObjectCapturingDoc(ObjectCapturingDoc):
    """面向schema的对象采集"""

    schema_name = StringField(required=True)

    meta = {
        'abstract': True,
        "indexes": [
            "schema_name"
        ]
    }

    MODELS = []

    @classmethod
    def post_captured(cls, **kwargs) -> NoReturn:
        docs: ["SchemaObjectCapturingDoc"] = kwargs["docs"]
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
    def capture(cls, model_to_capture: ["SchemaObjectCapturingDoc"] = None, **kwargs):
        if model_to_capture is None:
            model_to_capture = cls.MODELS
        cmdb_id: int = kwargs["cmdb_id"]
        task_record_id: int = kwargs["task_record_id"]
        cmdb_conn: OraclePlainConnector = kwargs["cmdb_connector"]
        schemas: [str] = kwargs["schemas"]

        for i, m in enumerate(model_to_capture):
            i += 1
            total = len(model_to_capture)
            print(f"* running {i} of {total}: {m.__doc__}")
            with schema_no_data("object") as schema_counter:
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
