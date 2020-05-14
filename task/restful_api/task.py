# Author: kk.Fang(fkfkbill@gmail.com)

from sqlalchemy.exc import DatabaseError

from utils.schema_utils import *
from task import utils
from task.const import *
from task.task_record import TaskRecord
from auth.const import PRIVILEGE
from auth.restful_api.base import PrivilegeReq, AuthReq
from restful_api.modules import as_view
from models.sqlalchemy import make_session
from oracle_cmdb.auth.user_utils import current_cmdb


@as_view(group="task")
class TaskHandler(PrivilegeReq):

    async def get(self):
        """任务列表"""

        self.acquire(PRIVILEGE.PRIVILEGE_TASK)

        params = self.get_query_args(Schema({
            scm_optional("keyword", default=None): scm_str,
            scm_optional("page", default=1): scm_int,
            scm_optional("per_page", default=10): scm_int,
            scm_optional("connect_name", default=None): scm_unempty_str,
            scm_optional("execution_status", default=None): And(
                scm_int, scm_one_of_choices(ALL_TASK_EXECUTION_STATUS)),
        }))
        keyword = params.pop("keyword")
        p = self.pop_p(params)
        connect_name = params.pop("connect_name")
        execution_status = params.pop("execution_status")
        del params
        with make_session() as session:
            task_q = session.query(CMDBTask,TaskRecord.status).\
                join(CMDBTask,CMDBTask.last_task_record_id==TaskRecord.task_record_id)
            if connect_name:
                task_q = task_q.filter(CMDBTask.connect_name == connect_name)
            if keyword:
                task_q = self.query_keyword(task_q, keyword,
                                            CMDBTask.connect_name,
                                            CMDBTask.status,
                                            TaskRecord.status,
                                            CMDBTask.cmdb_id)
            current_cmdb_ids = current_cmdb(session,self.current_user)
            if not self.is_admin():
                task_q = task_q.filter(CMDBTask.cmdb_id.in_(current_cmdb_ids))
            ret = await utils.get_task(
                task_q, execution_status=execution_status)
            items, p = self.paginate(ret, **p)
            items=[{**x[0].to_dict(), **{"execution_status": x[1]}} for x in items]
            self.resp(items, **p)

    def patch(self):
        """修改定时采集任务属性"""

        params = self.get_json_args(Schema({
            "id": scm_int,

            scm_optional("status"): scm_bool,
            scm_optional("schedule_time"): And(
                scm_unempty_str,
                lambda x: len(x) == 5
            ),
            scm_optional("frequency"): scm_int
        }))
        id = params.pop("id")
        with make_session() as session:
            session.query(CMDBTask).filter_by(id=id).update(params)
            self.resp_created()


@as_view("record", group="task")
class TaskRecordHandler(AuthReq):

    def get(self):
        """查询任务执行历史记录"""

        params = self.get_query_args(Schema({
            "cmdb_task_id": scm_int,
            scm_optional("page", default=1): scm_int,
            scm_optional("per_page", default=10): scm_int,
        }))
        p = self.pop_p(params)
        cmdb_task_id=params.pop("cmdb_task_id")

        with make_session() as session:
            task_exec_hist_q = session.query(CMDBTaskRecord,TaskRecord.status).\
                join(CMDBTask,CMDBTask.last_task_record_id==CMDBTaskRecord.task_record_id).\
                join(TaskRecord,CMDBTask.last_task_record_id==TaskRecord.task_record_id). \
                filter(CMDBTaskRecord.cmdb_task_id==cmdb_task_id).order_by(CMDBTaskRecord.cmdb_task_id.desc())
            items, p = self.paginate(task_exec_hist_q, **p)
            items = [{**x[0].to_dict(), **{"execution_status": x[1]}} for x in items]
            self.resp(items, **p)

    def delete(self):
        """手动删除挂起的任务"""

        params = self.get_json_args(Schema({
            "task_record_id": scm_int,
        }))
        task_record_id = params.pop("task_record_id")
        with make_session() as session:
            session.query(CMDBTaskRecord).filter_by(id=task_record_id).delete()
        self.resp_created(msg="done")


@as_view("execute", group="task")
class TaskManualExecuteHandler(AuthReq):

    def post(self):
        """手动运行任务"""

        params = self.get_json_args(Schema({
            "task_record_id": scm_int
        }))
        try:#TODO
            mkdata.run(
                params.pop("task_record_id"),
                use_queue=True,
                operator=self.current_user
            )
        except DatabaseError as e:
            return self.resp_bad_req(msg=str(e))
        self.resp_created({})


@as_view("flush_queue", group="task")
class FlushCeleryQHandler(AuthReq):

    def post(self):#TODO
        """清理队列中等待执行的任务"""

        self.acquire_admin()
        utils.flush_celery_q()
        self.resp_created(msg="已清理待采集队列")

