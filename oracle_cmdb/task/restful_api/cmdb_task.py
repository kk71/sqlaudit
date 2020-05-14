# Author: kk.Fang(fkfkbill@gmail.com)

import task.const
from restful_api import *
from models.sqlalchemy import *
from utils.schema_utils import *
from auth.const import PRIVILEGE
from cmdb.cmdb_task import *
from ...restful_api.base import *
from ...tasks.cmdb_capture import *


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

        params = self.get_json_args(Schema({
            "id": scm_int,

            scm_optional("status"): scm_bool,
            scm_optional("schedule_time"): And(
                scm_unempty_str,
                lambda x: len(x) == 5
            ),
            scm_optional("frequency"): scm_int
        }))
        the_id = params.pop("id")

        with make_session() as session:
            session.query(CMDBTask).filter_by(id=the_id).update(params)
        self.resp_created()


@as_view("record", group="task")
class CMDBTaskRecordHandler(OraclePrivilegeReq):

    def get(self):
        """纳管库任务历史记录"""
        self.acquire(PRIVILEGE.PRIVILEGE_TASK)

        params = self.get_query_args(Schema({
            "cmdb_task_id": scm_int,
            **self.gen_p()
        }))
        cmdb_task_id = params.pop("cmdb_task_id")
        p = self.pop_p(params)

        with make_session() as session:
            cmdb_task_record_q, qe = CMDBTaskRecord.\
                query_cmdb_task_record_with_task_record(session)
            cmdb_task_record_q = cmdb_task_record_q.filter(
                CMDBTaskRecord.cmdb_task_id == cmdb_task_id,
                CMDBTaskRecord.cmdb_id.in_(self.cmdb_ids(session))
            )
            rst, p = self.paginate(cmdb_task_record_q, **p)
            self.resp([i.to_dict() for i in rst], **p)

    def delete(self):
        """手动删除挂起的任务"""
        self.acquire(PRIVILEGE.PRIVILEGE_TASK)

        params = self.get_json_args(Schema({
            "task_record_id": scm_int,
        }))
        task_record_id = params.pop("task_record_id")
        with make_session() as session:
            session.query(CMDBTaskRecord).filter_by(id=task_record_id).delete()
        self.resp_created()


@as_view("execute", group="task")
class TaskManualExecuteHandler(OraclePrivilegeReq):

    def post(self):
        """手动运行任务"""
        self.acquire(PRIVILEGE.PRIVILEGE_TASK)

        params = self.get_json_args(Schema({
            "cmdb_task_id": scm_int
        }))
        task_record_id = OracleCMDBCaptureTask.shoot(
            operator=self.current_user,
            **params
        )
        self.resp_created({"task_record_id": task_record_id})


@as_view("flush_queue", group="task")
class FlushCeleryQHandler(OraclePrivilegeReq):

    def post(self):#TODO
        """清理队列中等待执行的任务"""
        self.acquire_admin()
        with make_session() as session:
            any_cmdb_task_object = session.query(CMDBTask).filter_by(
                task_type=task.const.TASK_TYPE_CAPTURE).first()
            any_cmdb_task_object.flush_celery_q()
        self.resp_created(msg="已清理待采集队列")

