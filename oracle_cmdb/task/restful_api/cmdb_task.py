# Author: kk.Fang(fkfkbill@gmail.com)

import task.const
from task.task_record import *
from restful_api import *
from models.sqlalchemy import *
from utils.schema_utils import *
from auth.const import PRIVILEGE
from ...restful_api.base import *
from oracle_cmdb.tasks.capture import *


@as_view(group="task")
class CMDBTaskHandler(OraclePrivilegeReq):

    def get(self):
        """纳管库任务列表"""
        self.acquire(PRIVILEGE.PRIVILEGE_TASK)

        params = self.get_query_args(Schema({
            scm_optional("execution_status", default=None): scm_empty_as_optional(And(
                scm_int,
                self.scm_one_of_choices(task.const.ALL_TASK_EXECUTION_STATUS)
            )),
            scm_optional("keyword", default=None): scm_str,
            **self.gen_p()
        }))
        keyword = params.pop("keyword")
        p = self.pop_p(params)
        execution_status = params.pop("execution_status")
        del params

        with make_session() as session:
            cmdb_task_q, qe = OracleCMDBTaskCapture.query_cmdb_task_with_last_record(session)
            cmdb_task_q = cmdb_task_q.filter(
                OracleCMDBTaskCapture.cmdb_id.in_(self.cmdb_ids(session)))
            if keyword:
                cmdb_task_q = self.query_keyword(cmdb_task_q, keyword,
                                                 OracleCMDBTaskCapture.cmdb_id,
                                                 OracleCMDBTaskCapture.connect_name)
            if execution_status is not None:
                cmdb_task_q = cmdb_task_q.filter(
                    TaskRecord.status == execution_status)
            rst, p = self.paginate(cmdb_task_q, **p)
            self.resp([qe.to_dict(i) for i in rst], **p)

    get.argument = {
        "querystring": {
            "execution_status": 1,
            "//keyword": "emm",
            "//page": 1,
            "//per_page": 10
        }
    }

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
            session.query(OracleCMDBTaskCapture).filter_by(id=the_id).update(params)
        self.resp_created()

    patch.argument = {
        "json": {
            "id": 1,
            "//status": True,
            "//schedule_time": "22:00",
            "//frequency": 10
        }
    }


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
            cmdb_task_record_q, qe = OracleCMDBTaskCaptureRecord. \
                query_cmdb_task_record_with_tsk_record(session)
            cmdb_task_record_q = cmdb_task_record_q.filter(
                OracleCMDBTaskCaptureRecord.cmdb_task_id == cmdb_task_id,
                OracleCMDBTaskCaptureRecord.cmdb_id.in_(self.cmdb_ids(session))
            )
            rst, p = self.paginate(cmdb_task_record_q, **p)
            self.resp([qe.to_dict(i) for i in rst], **p)

    get.argument = {
        "querystring": {
            "cmdb_task_id": 1991,
            "//page": 1,
            "//per_page": 10
        }
    }

    def delete(self):
        """手动删除挂起的任务"""
        self.acquire(PRIVILEGE.PRIVILEGE_TASK)

        params = self.get_query_args(Schema({
            "task_record_id": scm_int,
        }))
        task_record_id = params.pop("task_record_id")
        with make_session() as session:
            session.query(OracleCMDBTaskCaptureRecord).filter_by(
                task_record_id=task_record_id).delete()
        self.resp_created()

    delete.argument = {
        "querystring": {
            "task_record_id": 123
        }
    }


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

    post.argument = {
        "json": {
            "cmdb_task_id": 1991
        }
    }


@as_view("flush_queue", group="task")
class FlushCeleryQHandler(OraclePrivilegeReq):

    def post(self):
        """清理队列中等待执行的任务"""
        self.acquire_admin()
        with make_session() as session:
            any_cmdb_task_object = session.query(OracleCMDBTaskCapture).filter_by(
                task_type=task.const.TASK_TYPE_CAPTURE).first()
            any_cmdb_task_object.flush_celery_q()
        self.resp_created(msg="已清理待采集队列")

    post.argument = {}
