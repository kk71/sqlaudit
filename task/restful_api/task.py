# Author: kk.Fang(fkfkbill@gmail.com)

from schema import Schema,Optional
from sqlalchemy.exc import DatabaseError

from utils.schema_utils import *
from utils import const,cmdb_utils,task_utils
from task.task import *
from models.sqlalchemy import make_session
from restful_api.modules import as_view
from auth.restful_api.base import PrivilegeReq,AuthReq


@as_view("task", group="task")
class TaskHandler(PrivilegeReq):

    async def get(self):
        """获取任务列表"""

        self.acquire(const.PRIVILEGE.PRIVILEGE_TASK)

        params = self.get_query_args(Schema({
            Optional("keyword", default=None): scm_str,
            Optional("page", default=1): scm_int,
            Optional("per_page", default=10): scm_int,
            Optional("connect_name", default=None): scm_unempty_str,
            Optional("task_exec_scripts", default=const.DB_TASK_CAPTURE): scm_unempty_str,
            Optional("execution_status", default=None): And(
                scm_int, scm_one_of_choices(const.ALL_TASK_EXECUTION_STATUS)),
        }))
        keyword = params.pop("keyword")
        p = self.pop_p(params)
        connect_name = params.pop("connect_name")
        task_exec_scripts = params.pop("task_exec_scripts")
        execution_status = params.pop("execution_status")
        del params
        with make_session() as session:
            task_q = session.query(Task)
            if connect_name:
                task_q = task_q.filter(Task.connect_name == connect_name)
            if task_exec_scripts:
                task_q = task_q.filter(Task.task_exec_scripts == task_exec_scripts)
            if keyword:
                task_q = self.query_keyword(task_q, keyword,
                                            Task.connect_name,
                                            Task.group_name,
                                            Task.business_name,#TODO
                                            Task.server_name,
                                            Task.ip_address)
            current_cmdb_ids = cmdb_utils.get_current_cmdb(session, self.current_user)
            if not self.is_admin():
                task_q = task_q.filter(Task.cmdb_id.in_(current_cmdb_ids))
            ret = await task_utils.get_task(
                session, task_q, execution_status=execution_status)
            items, p = self.paginate(ret, **p)
            self.resp(items, **p)

    def patch(self):
        """修改任务状态"""
        params = self.get_json_args(Schema({
            "task_id": scm_int,

            Optional("task_status"): scm_bool,
            Optional("task_schedule_date"): And(scm_unempty_str, lambda x: len(x) == 5),
            Optional("task_exec_frequency"): scm_int
        }))
        task_id = params.pop("task_id")
        with make_session() as session:
            session.query(Task).filter_by(task_id=task_id).update(params)
            self.resp_created()

@as_view("taskexecutionhistory", group="task")
class TaskExecutionHistoryHandler(AuthReq):

    def get(self):
        """查询任务执行历史记录"""
        params = self.get_query_args(Schema({
            "task_id": scm_int,
            Optional("page", default=1): scm_int,
            Optional("per_page", default=10): scm_int,
        }))
        p = self.pop_p(params)
        with make_session() as session:
            task_exec_hist_q = session.query(TaskRecord). \
                filter_by(**params).order_by(TaskRecord.task_id.desc())
            items, p = self.paginate(task_exec_hist_q, **p)
            self.resp([i.to_dict() for i in items], **p)

    def delete(self):
        """手动删除挂起的任务"""
        params = self.get_json_args(Schema({
            "task_record_id": scm_int,
        }))
        task_record_id = params.pop("task_record_id")
        with make_session() as session:
            session.query(TaskRecord).filter_by(id=task_record_id).delete()
        self.resp_created(msg="done")

@as_view("taskmanualexecute", group="task")
class TaskManualExecute(AuthReq):

    def post(self):
        """手动运行任务"""
        params = self.get_json_args(Schema({
            "task_id": scm_int
        }))
        try:
            mkdata.run(
                params.pop("task_id"),
                use_queue=True,
                operator=self.current_user
            )
        except DatabaseError as e:
            return self.resp_bad_req(msg=str(e))
        self.resp_created({})

@as_view("flushceleryq", group="task")
class FlushCeleryQ(AuthReq):

    def post(self):
        """清理待采集队列"""
        self.acquire_admin()
        task_utils.flush_celery_q()
        self.resp_created(msg="已清理待采集队列")




