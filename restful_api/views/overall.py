# Author: kk.Fang(fkfkbill@gmail.com)

from schema import Schema, Optional, And, Or
from sqlalchemy import func

from .base import AuthReq
from utils.schema_utils import *
from utils.datetime_utils import *
from models.mongo import *
from models.oracle import *
from utils.const import *
from utils import cmdb_utils


class DashboardHandler(AuthReq):

    def get(self):
        """仪表盘"""
        with make_session() as session:
            cmdb_id_set = cmdb_utils.get_current_cmdb(session, self.current_user)

            print(list(session.query(TaskManage.cmdb_id, TaskManage.connect_name).all()))

            # cmdb_id_task_exec_hist_id = session.query(TaskManage.cmdb_id, func.first(TaskExecHistory.id)).\
            #     join(TaskExecHistory, TaskExecHistory.connect_name == TaskManage.connect_name).\
            #     filter(
            #         TaskManage.cmdb_id.in_(list(cmdb_id_set)),
            #         TaskManage.task_exec_scripts == DB_TASK_CAPTURE
            #     ).order_by(TaskExecHistory.task_end_date.desc()).group_by(TaskManage.cmdb_id)
            #
            # print(list(cmdb_id_task_exec_hist_id))

            self.resp({
                "sql_num": 0,
                "table_num": 0,
                "index_num": 0,
                "sequence_num": 0,

                "env": [],
                "cmdb_num": 0,
                "ai_tune_num": 0,
                "offline_ticket": {
                    "pending": 0,
                    "rejected": 0,
                    "passed": 0
                },

                "capture_tasks": [],
                "notice": ""
            })


class NoticeHandler(AuthReq):

    def post(self):
        """编辑公告栏内容"""
        param = self.get_json_args(Schema({
            "contents": scm_str
        }))
        with make_session() as session:
            notice = session.query(Notice).filter_by(notice_id=1).first()
            if not notice:
                notice = Notice()
            notice.from_dict(param)
            session.add(notice)
            session.commit()
            session.refresh(notice)
            self.resp_created(notice.to_dict())