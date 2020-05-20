# Author: kk.Fang(fkfkbill@gmail.com)

import rule.const
from models.sqlalchemy import *
from utils.schema_utils import *
from restful_api import *
from .base import *
from ..statistics import OracleStatsEntryCMDB, OracleStatsEntrySchema
from ..tasks.capture.cmdb_task_capture import *


@as_view("cmdb", group="dashboard", doc=False)
class EntryCMDBHandler(OraclePrivilegeReq):

    def get(self):
        """登录用户各库各维度对象数问题数和风险率"""
        params = self.get_query_args(Schema({
            "entry": self.scm_one_of_choices(rule.const.ALL_RULE_ENTRIES),
            **self.gen_p()
        }))
        p = self.pop_p(params)
        entry = params.pop("entry")

        with make_session() as session:
            last_success_task_record_id = OracleCMDBTaskCaptureRecord.\
                last_success_task_record_id(session)
            entry_cmdb_q = OracleStatsEntryCMDB.filter(
                task_record_id=last_success_task_record_id,
                entry=entry
            )
            ret, p = self.paginate(entry_cmdb_q, **p)
            self.resp([i.to_dict() for i in ret], **p)

    get.argument = {
        "querystring": {
            "entry": "SQL_TEXT",
            "//page": 1,
            "//per_page": 10
        }
    }


@as_view("schema", group="dashboard")
class EntryCMDBHandler(OraclePrivilegeReq):

    def get(self):
        """登录用户各库各schema各维度对象数问题数和风险率(仪表盘圆圈下钻)"""
        params = self.get_query_args(Schema({
            "entry": self.scm_one_of_choices(rule.const.ALL_RULE_ENTRIES),
            **self.gen_p()
        }))
        p = self.pop_p(params)
        entry = params.pop("entry")

        with make_session() as session:
            last_success_task_record_id = OracleCMDBTaskCaptureRecord. \
                last_success_task_record_id(session)
            entry_cmdb_q = OracleStatsEntrySchema.filter(
                task_record_id=last_success_task_record_id,
                entry=entry
            )
            ret, p = self.paginate(entry_cmdb_q, **p)
            self.resp([i.to_dict() for i in ret], **p)

    get.argument = {
        "querystring": {
            "entry": "SQL_TEXT",
            "//page": 1,
            "//per_page": 10
        }
    }

