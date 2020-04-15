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

        print("============== Schema Object capture ==============")

        print("=============== Schema SQL capture ===============")


