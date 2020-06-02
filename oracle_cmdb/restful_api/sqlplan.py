# Author: kk.Fang(fkfkbill@gmail.com)

from .base import OraclePrivilegeReq
from restful_api import *
from utils.schema_utils import *
from ..capture import OracleSQLPlanToday
from ..tasks.capture.cmdb_task_capture import OracleCMDBTaskCapture
from models.sqlalchemy import *
from ..cmdb import OracleCMDB


@as_view(group="online")
class SQLPlanHandler(OraclePrivilegeReq):

    def get(self):
        """线上审核的SQL执行计划，SQLPlus风格，文本形式的表格"""
        params = self.get_query_args(Schema({
            "cmdb_id": scm_int,
            "sql_id": scm_unempty_str,
            "plan_hash_value": scm_int,
        }))
        cmdb_id = params.pop("cmdb_id")
        with make_session() as session:
            the_cmdb = self.cmdbs(session).filter(
                OracleCMDB.cmdb_id == cmdb_id).first()
            the_cmdb_task = OracleCMDBTaskCapture.get_cmdb_task_by_cmdb(the_cmdb)
            s = OracleSQLPlanToday.sql_plan_table(
                task_record_id=the_cmdb_task.last_success_task_record_id,
                **params
            )
            self.resp({
                "plan": s
            })

    get.argument = {
        "querystring": {
            "cmdb_id": "2526",
            "sql_id": "2h37v66c9spu6",
            "plan_hash_value": "2959612647"
        }
    }
