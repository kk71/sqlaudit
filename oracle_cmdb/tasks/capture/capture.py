# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleCMDBCaptureTask"
]

from typing import NoReturn

import task.const
from utils.datetime_utils import *
from cmdb.cmdb_task import *
from task.task import *
from oracle_cmdb.capture.base import *
from oracle_cmdb.cmdb import *
from oracle_cmdb.plain_db import *
from models.sqlalchemy import *
from oracle_cmdb.issue import OracleOnlineIssue
from oracle_cmdb.statistics.base import OracleBaseStatistics


@register_task(task.const.TASK_TYPE_CAPTURE)
class OracleCMDBCaptureTask(BaseCMDBTask):
    """纳管库采集（包括采集、分析、统计三步骤）"""

    @classmethod
    def task(cls, task_record_id: int, **kwargs):
        cmdb_task_id: int = kwargs["cmdb_task_id"]

        # 记录当前时间，后面的snapshot id以该时间为准
        now = arrow.now()
        print(f"* now is {dt_to_str(now)}")

        with make_session() as session:
            the_cmdb_task = session.query(CMDBTask).filter_by(
                id=cmdb_task_id).first()
            the_cmdb = session.query(OracleCMDB).filter_by(
                cmdb_id=the_cmdb_task.cmdb_id).first()
            cmdb_id = the_cmdb.cmdb_id
            schemas: [str] = the_cmdb.related_schemas()
            cmdb_conn: OraclePlainConnector = the_cmdb.build_connector()
            print(f"{len(schemas)} schema(s) to run: {schemas}")

        BaseOracleCapture.collect()

        print("================== CMDB capture ==================")
        OracleObjectCapturingDoc.process(
            cmdb_id=cmdb_id,
            task_record_id=task_record_id,
            cmdb_connector=cmdb_conn
        )

        print("============== Schema Object capture ==============")
        OracleSchemaObjectCapturingDoc.process(
            cmdb_id=cmdb_id,
            task_record_id=task_record_id,
            cmdb_connector=cmdb_conn,
            schemas=schemas
        )

        print("=============== Schema SQL capture ===============")
        OracleSQLCapturingDoc.process(
            cmdb_id=cmdb_id,
            task_record_id=task_record_id,
            cmdb_connector=cmdb_conn,
            schemas=schemas,
            now=now
        )

        print("=========== Schema SQL two-days capture ===========")
        OracleTwoDaysSQLCapturingDoc.process(
            cmdb_id=cmdb_id,
            task_record_id=task_record_id,
            cmdb_connector=cmdb_conn,
            schemas=schemas,
            now=now
        )

        print("================== Rule Analyse ==================")
        OracleOnlineIssue.collect()
        OracleOnlineIssue.process(
            cmdb_id=cmdb_id,
            task_record_id=task_record_id,
            schemas=schemas
        )

        print("============ Make Statistics Data ============")
        OracleBaseStatistics.collect()
        OracleBaseStatistics.process(
            cmdb_id=cmdb_id,
            task_record_id=task_record_id,
            schemas=schemas
        )

    @classmethod
    def schedule(
            cls,
            now: arrow.Arrow,
            scheduler_starting_time: arrow.Arrow,
            **kwargs) -> NoReturn:
        print("yay")
