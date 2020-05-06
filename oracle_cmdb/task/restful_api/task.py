

from sqlalchemy.exc import DatabaseError

from ...auth.user_utils import current_cmdb
from utils.schema_utils import *
from task import utils
from task.const import *
from cmdb.task import CMDBTask,CMDBTaskRecord
from auth.const import PRIVILEGE
from auth.restful_api.base import PrivilegeReq, AuthReq
from restful_api.modules import as_view
from models.sqlalchemy import make_session


@as_view(group="oracle_task")
class TaskHandler(PrivilegeReq):

    async def get(self):
        """与oracle相关,任务列表"""

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
            task_q = session.query(CMDBTask)
            if connect_name:
                task_q = task_q.filter(CMDBTask.connect_name == connect_name)
            if keyword:
                task_q = self.query_keyword(task_q, keyword,
                                            CMDBTask.connect_name,
                                            CMDBTask.status,
                                            CMDBTask.execution_status,
                                            CMDBTask.cmdb_id)
            current_cmdb_ids = current_cmdb(self.current_user)
            if not self.is_admin():
                task_q = task_q.filter(CMDBTask.cmdb_id.in_(current_cmdb_ids))
            ret = await utils.get_task(
                task_q, execution_status=execution_status)
            items, p = self.paginate(ret, **p)
            self.resp(items, **p)

    def patch(self):
        """与oracle相关,修改定时采集任务属性"""

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


@as_view("record", group="oracle_task")
class TaskRecordHandler(AuthReq):

    def get(self):
        """与oracle相关,查询任务执行历史记录"""

        params = self.get_query_args(Schema({
            "cmdb_task_id": scm_int,
            scm_optional("page", default=1): scm_int,
            scm_optional("per_page", default=10): scm_int,
        }))
        p = self.pop_p(params)

        with make_session() as session:
            task_exec_hist_q = session.query(CMDBTaskRecord). \
                filter_by(**params).order_by(CMDBTaskRecord.cmdb_task_id.desc())
            items, p = self.paginate(task_exec_hist_q, **p)
            self.resp([i.to_dict() for i in items], **p)

    def delete(self):
        """与oracle相关,手动删除挂起的任务"""

        params = self.get_json_args(Schema({
            "task_record_id": scm_int,
        }))
        task_record_id = params.pop("task_record_id")
        with make_session() as session:
            session.query(CMDBTaskRecord).filter_by(id=task_record_id).delete()
        self.resp_created(msg="done")


@as_view("execute", group="oracle_task")
class TaskManualExecuteHandler(AuthReq):

    def post(self):
        """与oracle相关,手动运行任务"""

        params = self.get_json_args(Schema({
            "task_id": scm_int
        }))
        try:#TODO
            mkdata.run(
                params.pop("task_id"),
                use_queue=True,
                operator=self.current_user
            )
        except DatabaseError as e:
            return self.resp_bad_req(msg=str(e))
        self.resp_created({})


@as_view("flush_queue", group="oracle_task")
class FlushCeleryQHandler(AuthReq):

    def post(self):#TODO
        """与oracle相关,清理队列中等待执行的任务"""

        self.acquire_admin()
        utils.flush_celery_q()
        self.resp_created(msg="已清理待采集队列")
