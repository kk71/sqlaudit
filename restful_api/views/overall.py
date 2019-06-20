# Author: kk.Fang(fkfkbill@gmail.com)

from schema import Schema, Optional, And, Or
from sqlalchemy import func

from .base import AuthReq
from utils.schema_utils import *
from utils.perf_utils import timing
from models.mongo import *
from models.oracle import *
from utils.const import *
from utils import cmdb_utils


class DashboardHandler(AuthReq):

    @timing()
    def get(self):
        """仪表盘"""
        with make_session() as session:
            cmdb_id_set = cmdb_utils.get_current_cmdb(session, self.current_user)
            # 获取每个库最后一次抓取分析成功的历史记录的id
            sub_q = session.\
                query(TaskExecHistory.id.label("id"), TaskManage.cmdb_id.label("cmdb_id")).\
                join(TaskExecHistory, TaskExecHistory.connect_name == TaskManage.connect_name).\
                filter(TaskManage.cmdb_id.in_(list(cmdb_id_set)),
                       TaskManage.task_exec_scripts == DB_TASK_CAPTURE,
                       TaskExecHistory.status == True).subquery()
            cmdb_id_exec_hist_id_list_q = session.\
                query(sub_q.c.cmdb_id, func.max(sub_q.c.id)).group_by(sub_q.c.cmdb_id)
            task_exec_hist_id_list: [str] = [str(i[1]) for i in cmdb_id_exec_hist_id_list_q]
            self.get.tik("received task_exec_hist_id_list")

            # 计算值
            sql_num = len(SQLText.filter_by_exec_hist_id(task_exec_hist_id_list).distinct("sql_id"))
            table_num = ObjTabInfo.filter_by_exec_hist_id(task_exec_hist_id_list).count()
            index_num = ObjIndColInfo.filter_by_exec_hist_id(task_exec_hist_id_list).count()
            self.get.tik("calced 3 count")

            # 维度的数据库
            envs = session.query(Param.param_value, func.count(CMDB.cmdb_id)).\
                filter(Param.param_id == CMDB.domain_env,
                       Param.param_type == PARAM_TYPE_ENV).\
                group_by(Param.param_value)
            # 线下审核工单状态归类
            offline_tickets = session.query(
                WorkList.work_list_status, func.count(WorkList.work_list_id)).\
                group_by(WorkList.work_list_status)
            offline_status_desc = {
                0: "待审核",
                1: "审核通过",
                2: "被驳回",
                3: "已上线"
            }
            self.get.tik("finished offline ticket")
            # 线上审核的采集任务
            capture_tasks = session.query(TaskExecHistory.status, TaskExecHistory.task_id).\
                filter(TaskExecHistory.id.in_([int(i) for i in task_exec_hist_id_list]))
            task_status_desc = {
                None: "正在执行",
                True: "成功",
                False: "失败"
            }
            task_status = {i: 0 for i in task_status_desc.values()}
            for status, task_id in capture_tasks:
                if task_id:
                    task_status[task_status_desc[status]] += 1
            self.get.tik("finished capture tasks status count")
            # 公告板
            notice = session.query(Notice).filter(Notice.notice_id == 1).first()
            self.get.tik("finished notice")
            self.resp({
                "sql_num": sql_num,
                "table_num": table_num,
                "index_num": index_num,
                "sequence_num": 0,

                "env": self.dict_to_verbose_dict_in_list(dict(envs)),
                "cmdb_num": session.query(CMDB).count(),
                "ai_tune_num": 0,
                "offline_ticket": {offline_status_desc[k]: v for k, v in dict(offline_tickets).items()},

                "capture_tasks": self.dict_to_verbose_dict_in_list(task_status),
                "notice": notice.contents if notice else ""
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
            notice.update_user = self.current_user
            session.add(notice)
            session.commit()
            session.refresh(notice)
            self.resp_created(notice.to_dict())
