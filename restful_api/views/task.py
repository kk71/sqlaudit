# Author: kk.Fang(fkfkbill@gmail.com)

from schema import Schema, Optional, And
from cx_Oracle import DatabaseError

from .base import AuthReq, PrivilegeReq
from models.oracle import *
from utils.schema_utils import *
from utils import cmdb_utils, const, task_utils, score_utils
from utils.conc_utils import *
from past import mkdata


class TaskHandler(PrivilegeReq):

    async def get(self):
        """获取任务列表"""

        self.acquire(const.PRIVILEGE.PRIVILEGE_TASK)

        params = self.get_query_args(Schema({
            Optional("keyword", default=None): scm_str,
            Optional("page", default=1): scm_int,
            Optional("per_page", default=10): scm_int,
            Optional("connect_name", default=None): scm_unempty_str,
            Optional("task_exec_scripts", default=None): scm_unempty_str,
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
            task_q = session.query(TaskManage)
            if connect_name:
                task_q = task_q.filter(TaskManage.connect_name == connect_name)
            if task_exec_scripts:
                task_q = task_q.filter(TaskManage.task_exec_scripts == task_exec_scripts)
            if keyword:
                task_q = self.query_keyword(task_q, keyword,
                                            TaskManage.connect_name,
                                            TaskManage.group_name,
                                            TaskManage.business_name,
                                            TaskManage.server_name,
                                            TaskManage.ip_address)
            current_cmdb_ids = cmdb_utils.get_current_cmdb(session, self.current_user)
            if not self.is_admin():
                task_q = task_q.filter(TaskManage.cmdb_id.in_(current_cmdb_ids))
            ret = await task_utils.get_task(
                session, task_q, cmdb_ids=current_cmdb_ids, execution_status=execution_status)
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
            session.query(TaskManage).filter_by(task_id=task_id).update(params)
            self.resp_created()


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
            task_exec_hist_q = session.query(TaskExecHistory). \
                filter_by(**params).order_by(TaskExecHistory.id.desc())
            items, p = self.paginate(task_exec_hist_q, **p)
            self.resp([i.to_dict() for i in items], **p)


class TaskManualExecute(AuthReq):

    def post(self):
        """手动运行任务"""
        params = self.get_json_args(Schema({
            "task_id": scm_int
        }))
        try:
            mkdata.run(params.pop("task_id"), use_queue=True)
        except DatabaseError as e:
            return self.resp_bad_req(msg=str(e))
        self.resp_created({})


class FlushCeleryQ(AuthReq):

    def post(self):
        """清理待采集队列"""
        self.acquire_admin()
        task_utils.flush_celery_q()
        self.resp_created(msg="已清理待采集队列")


