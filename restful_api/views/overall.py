# Author: kk.Fang(fkfkbill@gmail.com)

from schema import Schema, Optional, And
from sqlalchemy import func
from mongoengine import Q

from .base import AuthReq, PrivilegeReq
from utils.schema_utils import *
from utils.perf_utils import timing
from models.mongo import *
from models.oracle import *
from utils.const import *
from utils import cmdb_utils, object_utils
from utils.cmdb_utils import get_current_cmdb, get_current_schema
from utils import const

from models.oracle.optimize import *
from restful_api.views.offline import OfflineTicketCommonHandler


class DashboardHandler(PrivilegeReq):

    @timing()
    def get(self):
        """仪表盘"""
        with make_session() as session:
            # 计算值
            sql_num, table_num, index_num, task_exec_hist_id_list = object_utils. \
                dashboard_3_sum(session=session, login_user=self.current_user)

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
            offline_tickets = OfflineTicketCommonHandler.privilege_filter_ticket(
                self=self, q=offline_tickets)
            offline_status_desc = {
                0: "待审核",
                1: "审核通过",
                2: "被驳回",
                3: "已上线",
                4: "上线失败"
            }

            # 线上审核的采集任务
            task_status_desc = {
                None: "正在执行",
                True: "成功",
                False: "失败",
                "no": "从未执行"
            }
            cmdb_ids = cmdb_utils.get_current_cmdb(session, self.current_user)
            capture_tasks_status = {i[0]: "no" for i in session.query(TaskManage.task_id).
                filter(TaskManage.cmdb_id.in_(cmdb_ids),
                       TaskManage.task_exec_scripts == DB_TASK_CAPTURE)}
            for hist in session.query(TaskExecHistory.task_id, TaskExecHistory.status). \
                    filter(TaskExecHistory.id.in_(task_exec_hist_id_list)):
                task_id, status = hist
                capture_tasks_status[task_id] = status

            task_status = {i: 0 for i in task_status_desc.values()}
            for task_id, status in capture_tasks_status.items():
                if task_id:
                    task_status[task_status_desc[status]] += 1
            # 公告板
            notice = session.query(Notice).filter(Notice.notice_id == 1).first()
            self.resp({
                "sql_num": sql_num,
                "table_num": table_num,
                "index_num": index_num,
                "sequence_num": ObjSeqInfo.objects(
                    cmdb_id__in=cmdb_ids, task_record_id__in=task_exec_hist_id_list).count(),
                "env": self.dict_to_verbose_dict_in_list(dict(envs)),
                "cmdb_num": len(cmdb_ids),
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

        with make_session() as session:
            last_success_task_record_id = session.query(TaskExecHistory.id). \
                join(CMDB, TaskExecHistory.connect_name == CMDB.connect_name). \
                filter(CMDB.cmdb_id == params["cmdb_id"], TaskExecHistory.status == True). \
                order_by(TaskExecHistory.id.desc()). \
                first()
            if last_success_task_record_id:
                last_success_task_record_id = last_success_task_record_id[0]
            else:
                print("no task exec hist was found.")
                return self.resp([])

        obj_table_info_q = ObjTabInfo. \
            filter_by_exec_hist_id(last_success_task_record_id). \
            filter(**params). \
            order_by("-etl_date")
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


class StatsNumDrillDownHandler(AuthReq):

    def get(self):
        """仪表盘四个数据的下钻信息"""
        params = self.get_query_args(Schema({
            "drill_down_type": scm_one_of_choices(const.ALL_DASHBOARD_STATS_NUM_TYPE),
            **self.gen_p()
        }))
        p = self.pop_p(params)
        with make_session() as session:
            cmdb_ids = get_current_cmdb(session, self.current_user)
            Qs=None
            for cmdb_id in cmdb_ids:
                schema_name = get_current_schema(session, self.current_user, cmdb_id)
                if not Qs:
                    Qs=Q(**{f"cmdb_id":cmdb_id,f"schema_name__in":schema_name})
                else:
                    Qs=Qs |  Q(**{f"cmdb_id":cmdb_id,f"schema_name__in":schema_name})
            if not Qs:
                return self.resp([])
            drill_down_q = StatsDashboardDrillDown.objects(
                        **params).filter(Qs)

            drill_down, p = self.paginate(drill_down_q, **p)
            self.resp([x.to_dict() for x in drill_down], **p)






