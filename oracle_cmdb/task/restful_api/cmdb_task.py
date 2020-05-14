# Author: kk.Fang(fkfkbill@gmail.com)

import task.const
from restful_api import *
from models.sqlalchemy import *
from utils.schema_utils import *
from auth.const import PRIVILEGE
from cmdb.cmdb_task import *
from ...restful_api.base import *


@as_view(group="task")
class CMDBTaskHandler(OraclePrivilegeReq):

    def get(self):
        """纳管库任务列表"""
        self.acquire(PRIVILEGE.PRIVILEGE_TASK)

        params = self.get_query_args(Schema({
            scm_optional("execution_status", default=None): And(
                scm_int,
                self.scm_one_of_choices(task.const.ALL_TASK_EXECUTION_STATUS)
            ),
            scm_optional("keyword", default=None): scm_str,
            **self.gen_p()
        }))
        keyword = params.pop("keyword")
        p = self.pop_p(params)
        execution_status = params.pop("status")
        del params

        with make_session() as session:
            cmdb_task_q, qe = CMDBTask.query_cmdb_task_with_last_record(session)
            cmdb_task_q = cmdb_task_q.filter(
                CMDBTask.cmdb_id.in_(self.cmdb_ids(session)))
            if keyword:
                cmdb_task_q = self.query_keyword(cmdb_task_q, keyword,
                                                 CMDBTask.cmdb_id,
                                                 CMDBTask.connect_name)
            if execution_status is not None:
                cmdb_task_q = cmdb_task_q.filter(CMDBTask.status == execution_status)
            rst, p = self.paginate(cmdb_task_q, **p)
            self.resp([qe.to_dict(i) for i in rst], **p)

    def patch(self):
        """修改纳管库任务"""
        self.acquire(PRIVILEGE.PRIVILEGE_TASK)
