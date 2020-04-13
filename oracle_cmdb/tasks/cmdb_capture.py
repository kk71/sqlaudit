# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleCMDBCaptureTask"
]

import task.const
from cmdb.task import *
from cmdb.cmdb_task import *
from task.task import *
from rule.rule import CMDBRule
from ..capture import modules
from ..capture.base import *
from ..cmdb import *
from ..plain_db import *
from models.sqlalchemy import *


@register_task(task.const.TASK_TYPE_CAPTURE)
class OracleCMDBCaptureTask(BaseCMDBTask):

    """纳管库采集（包括采集、分析、统计三步骤）"""

    @classmethod
    def cmdb_capture(
            cls,
            cmdb_conn: OraclePlainConnector):
        """采集与schema无关的纳管库数据"""
        for i, m in enumerate(modules.CMDB_MODELS_TO_COLLECT):
            i += 1
            total = len(modules.CMDB_MODELS_TO_COLLECT)
            print(f"* running {i} of {total}: {m.__doc__}")
            sql_to_run = m.simple_capture()
            docs = [
                m(**c)
                for c in cmdb_conn.select_dict(sql_to_run, one=False)]
            if not docs:
                print("no data captured.")
                continue
            m.post_captured(docs=docs)
            docs_inserted = m.objects.insert(docs)
            print(f"{len(docs_inserted)} captured.")

    @classmethod
    def schema_object_capture(
            cls,
            schemas: [str],
            cmdb_conn: OraclePlainConnector):
        """逐schema采集object数据"""
        for i, m in enumerate(
                modules.SCHEMA_OBJ_MODELS_TO_COLLECT):
            i += 1
            total = len(modules.SCHEMA_OBJ_MODELS_TO_COLLECT)
            print(f"* running {i} of {total}: {m.__doc__}")
            for a_schema in schemas:
                sql_to_run = m.simple_capture(obj_owner=a_schema)
                docs = [
                    m(**c)
                    for c in cmdb_conn.select_dict(sql_to_run, one=False)]
                if not docs:
                    print(f"no data captured in {a_schema}.")
                    continue
                m.post_captured(
                    obj_owner=a_schema,
                    docs=docs,
                    cmdb_connector=cmdb_conn
                )
                docs_inserted = m.objects.insert(docs)
                print(f"{len(docs_inserted)} captured in {a_schema}.")

    @classmethod
    def schema_sql_capture(
            cls,
            the_cmdb: OracleCMDB,
            the_schema: str,
            the_model: SQLCapturingDoc):
        pass

    @classmethod
    def rule_analyse(
            cls,
            the_cmdb: OracleCMDB,
            the_model: ObjectCapturingDoc,
            the_rule: CMDBRule
    ):
        pass

    @classmethod
    def make_statistics(cls):
        pass

    @classmethod
    def task(cls, task_record_id: int, **kwargs):
        cmdb_task_id: int = kwargs["cmdb_task_id"]

        with make_session() as session:
            the_cmdb_task = session.query(CMDBTask).filter_by(
                id=cmdb_task_id).first()
            the_cmdb = session.query(OracleCMDB).filter_by(
                cmdb_id=the_cmdb_task.cmdb_id).first()
            cmdb_conn = the_cmdb.build_connector()
            schemas: [str] = the_cmdb.get_bound_schemas(session)
            print(f"{len(schemas)} schema(s) to run: {schemas}")

        modules.collect_dynamic_modules()
        print("================== CMDB capture ==================")
        cls.cmdb_capture(cmdb_conn)

        print("============== Schema Object capture ==============")
        cls.schema_object_capture(schemas, cmdb_conn)

        print("=============== Schema SQL capture ===============")


