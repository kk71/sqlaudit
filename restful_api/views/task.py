# Author: kk.Fang(fkfkbill@gmail.com)

from schema import Schema, Optional, And

from .base import AuthReq, PrivilegeReq
from models.oracle import *
from utils.schema_utils import *
from utils import cmdb_utils, const, task_utils, score_utils
from past import mkdata


class TaskHandler(PrivilegeReq):

    def get(self):
        """获取任务列表"""

        self.acquire(const.PRIVILEGE.PRIVILEGE_TASK)

        params = self.get_query_args(Schema({
            Optional("keyword", default=None): scm_str,
            Optional("page", default=1): scm_int,
            Optional("per_page", default=10): scm_int,
            Optional("connect_name", default=None): scm_unempty_str,
            Optional("task_exec_scripts", default=None): scm_unempty_str,
            # Optional("last_result", default=None): scm_bool,
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
            ret = []
            pending_task_ids: set = task_utils.get_pending_task()
            cmdb_capture_task_latest_task_id = score_utils.get_latest_task_record_id(
                session,
                cmdb_id=current_cmdb_ids,
                status=None  # None表示不过滤状态
            )
            for t in task_q:
                t_dict = t.to_dict()
                t_dict["last_result"] = t_dict["execution_status"] = const.TASK_NEVER_RAN
                last_task_exec_history = session.query(TaskExecHistory).filter(
                    TaskExecHistory.id == cmdb_capture_task_latest_task_id.get(t.cmdb_id, None)
                )
                if last_task_exec_history:
                    t_dict["last_result"] = last_task_exec_history.status
                if execution_status == const.TASK_NEVER_RAN:
                    if t_dict["last_result"] is not const.TASK_NEVER_RAN:
                        continue
                elif execution_status == const.TASK_PENDING:
                    if t.task_id not in pending_task_ids:
                        continue
                elif execution_status == const.TASK_RUNNING:
                    if t_dict["last_result"] is not None:
                        continue
                elif execution_status == const.TASK_DONE:
                    if t_dict["last_result"] is True:
                        continue
                elif execution_status == const.TASK_FAILED:
                    if t_dict["last_result"] is not False:
                        continue

                if t_dict["last_result"] is None:
                    t_dict["execution_status"] = const.TASK_RUNNING
                elif t.task_id in pending_task_ids:
                    t_dict["execution_status"] = const.TASK_PENDING
                elif t_dict["last_result"] is True:
                    t_dict["execution_status"] = const.TASK_DONE
                elif t_dict["last_result"] is False:
                    t_dict["execution_status"] = const.TASK_FAILED

                ret.append(t_dict)
            ret = sorted(ret,
                         key=lambda k: 0 if k["last_result"] is None
                         else k["last_result"], reverse=True)
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
        mkdata.run(params.pop("task_id"), use_queue=True)
        self.resp_created({})
