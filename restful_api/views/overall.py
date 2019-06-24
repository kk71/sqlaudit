# Author: kk.Fang(fkfkbill@gmail.com)

from schema import Schema, Optional, And, Or
from sqlalchemy import func

from .base import AuthReq
from utils.schema_utils import *
from utils.perf_utils import timing
from models.mongo import *
from models.oracle import *
from utils.const import *
from utils import cmdb_utils, object_utils
from models.oracle.optimize import *


class DashboardHandler(AuthReq):

    @timing()
    def get(self):
        """仪表盘"""
        with make_session() as session:
            # 计算值
            sql_num, table_num, index_num, task_exec_hist_id_list = object_utils.\
                dashboard_3_sum(session, self.current_user)

            # 维度的数据库
            envs = session.query(Param.param_value, func.count(CMDB.cmdb_id)). \
                filter(Param.param_id == CMDB.domain_env,
                       Param.param_type == PARAM_TYPE_ENV). \
                group_by(Param.param_value)
            # 智能优化执行次数
            optimized_execution_times = 0
            optimized_execution_q = list(session.query(AituneResultDetails).with_entities(func.count()))
            if optimized_execution_q:
                optimized_execution_times = optimized_execution_q[0][0]

            # 线下审核工单状态归类
            offline_tickets = session.query(
                WorkList.work_list_status, func.count(WorkList.work_list_id)). \
                group_by(WorkList.work_list_status)
            offline_status_desc = {
                0: "待审核",
                1: "审核通过",
                2: "被驳回",
                3: "已上线",
                4: "上线失败"
            }
            # 线上审核的采集任务
            capture_tasks = session.query(TaskExecHistory.status, TaskExecHistory.task_id). \
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
            # 公告板
            notice = session.query(Notice).filter(Notice.notice_id == 1).first()
            self.resp({
                "sql_num": sql_num,
                "table_num": table_num,
                "index_num": index_num,
                "sequence_num": 0,
                "env": self.dict_to_verbose_dict_in_list(dict(envs)),
                "cmdb_num": session.query(CMDB).count(),
                "ai_tune_num": optimized_execution_times,
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


class MetadataListHandler(AuthReq):

    def get(self):
        """元数据查询"""
        params = self.get_query_args(Schema({
            "cmdb_id": scm_int,
            "schema_name": scm_unempty_str,
            Optional("table_name"): scm_unempty_str,
            Optional("search_type", default="exact"): And(
                scm_str, scm_one_of_choices(("icontains", "exact"))),

            Optional("keyword", default=None): scm_str,
            **self.gen_p()
        }))
        search_type = params.pop('search_type')
        if params.get("table_name", None):
            params[f"table_name__{search_type}"] = params.pop('table_name')
        keyword = params.pop("keyword")
        p = self.pop_p(params)

        obj_table_info_q = ObjTabInfo.objects.filter(**params).order_by("-etl_date")
        if keyword:
            obj_table_info_q = self.query_keyword(obj_table_info_q, keyword, "schema_name",
                                                  "ip_address",
                                                  "sid",
                                                  "table_name",
                                                  "table_type",
                                                  "object_type",
                                                  "iot_name")
        items, p = self.paginate(obj_table_info_q, **p)
        self.resp([i.to_dict() for i in items], **p)
