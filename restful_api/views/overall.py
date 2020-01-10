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
from utils import cmdb_utils
from utils.cmdb_utils import get_current_cmdb, get_current_schema
from utils import const, score_utils, task_utils
from utils.conc_utils import async_thr
from models.oracle.optimize import *
from utils.offline_utils import *


class DashboardHandler(PrivilegeReq):

    @timing()
    async def get(self):
        """仪表盘"""
        with make_session() as session:
            # 顶部四个统计数字
            stats_login_user = StatsLoginUser.objects(login_user=self.current_user).\
                order_by("-etl_date").first()
            if stats_login_user:
                stats_num_dict = stats_login_user.to_dict()
            else:
                stats_num_dict = StatsLoginUser().to_dict()

            # 维度的数据库
            cmdb_ids = await async_thr(cmdb_utils.get_current_cmdb, session,
                                       self.current_user)
            envs = session.query(CMDB.group_name, func.count(CMDB.cmdb_id)). \
                filter(CMDB.cmdb_id.in_(cmdb_ids)). \
                group_by(CMDB.group_name)
            # 智能优化执行次数
            optimized_execution_times = 0
            optimized_execution_q = list(
                session.query(AituneResultDetails).with_entities(func.count()))
            if optimized_execution_q:
                optimized_execution_times = optimized_execution_q[0][0]

            # 线下审核工单状态归类
            offline_tickets = session.query(
                WorkList.work_list_status, func.count(WorkList.work_list_id)). \
                group_by(WorkList.work_list_status)
            offline_tickets = TicketReq.privilege_filter_ticket(
                self=self, q=offline_tickets)  # 不是个特别好的操作，但不会出问题。

            # 线上审核的采集任务
            task_q = session.query(TaskManage).\
                filter(TaskManage.task_exec_scripts == const.DB_TASK_CAPTURE)
            if not self.is_admin():
                task_q = task_q.filter(TaskManage.cmdb_id.in_(cmdb_ids))
            tasks = await task_utils.get_task(
                session, task_q, execution_status=None)
            task_status = {
                k: 0 for k in const.ALL_TASK_EXECUTION_STATUS_CHINESE_MAPPING.values()
            }
            for t in tasks:
                task_status[
                    const.ALL_TASK_EXECUTION_STATUS_CHINESE_MAPPING[
                        t["execution_status"]]] += 1

            # 公告板
            notice = session.query(Notice).filter(Notice.notice_id == 1).first()
            self.resp({
                **stats_num_dict,
                "env": self.dict_to_verbose_dict_in_list(dict(envs)),
                "cmdb_num": len(cmdb_ids),
                "ai_tune_num": optimized_execution_times,
                "offline_ticket": {ALL_OFFLINE_TICKET_STATUS_CHINESE[k]: v
                                   for k, v in dict(offline_tickets).items()},
                "capture_tasks": self.dict_to_verbose_dict_in_list(task_status),
                "all_capture_task_num": task_q.count(),
                "notice": notice.contents if notice else "",
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


class MetadataListHandler(PrivilegeReq):

    def get(self):
        """元数据查询"""

        self.acquire(PRIVILEGE.PRIVILEGE_METADATA)

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
            if "." in params["table_name"]:
                print(f"warning: the original table_name is {params['table_name']} "
                      f"the word before the dot is recognized as schema and has been ignored.")
                params["table_name"] = params["table_name"].split(".")[1]
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

    async def get(self):
        """仪表盘四个数据的下钻信息"""
        params = self.get_query_args(Schema({
            "drill_down_type": And(scm_dot_split_str,
                                   scm_subset_of_choices(const.ALL_STATS_NUM_TYPE)),
            Optional("cmdb_id", default=None): scm_int,
            Optional("schema_name", default=None): scm_unempty_str,
            **self.gen_p()
        }))
        ddt_in = params.pop("drill_down_type")
        the_cmdb_id = params.pop("cmdb_id")
        the_schema_name = params.pop("schema_name")
        p = self.pop_p(params)
        if not the_cmdb_id and the_schema_name:
            return self.resp_bad_req(msg="参数错误，指定schema则必须指定纳管库")
        with make_session() as session:
            if not the_cmdb_id:
                cmdb_ids = get_current_cmdb(session, self.current_user)
            else:
                cmdb_ids = [the_cmdb_id]
            cmdb_id_task_record_id = await async_thr(
                score_utils.get_latest_task_record_id, session, cmdb_id=cmdb_ids)
            Qs = None
            for cmdb_id in cmdb_ids:
                try:
                    if the_schema_name:
                        schema_name = [the_schema_name]
                    else:
                        schema_name = await async_thr(
                            get_current_schema, session, self.current_user, cmdb_id)
                    if not Qs:
                        Qs = Q(**{
                            "cmdb_id": cmdb_id,
                            "schema_name__in": schema_name,
                            "task_record_id": cmdb_id_task_record_id[cmdb_id]})
                    else:
                        Qs = Qs | Q(**{
                            "cmdb_id": cmdb_id,
                            "schema_name__in": schema_name,
                            "task_record_id": cmdb_id_task_record_id[cmdb_id]})
                except:
                    continue
            if not Qs:
                return self.resp([])
            drill_down_q = StatsNumDrillDown.objects(
                drill_down_type__in=ddt_in,
                # job_id__ne=None,
                num__ne=0,
                # score__nin=[None, 0, 100]
            ).filter(Qs).filter(Q(num_with_risk__ne=0) | Q(problem_num__ne=0)).\
                order_by("-etl_date")
            items, p = self.paginate(drill_down_q, **p)
            self.resp([x.to_dict() for x in items], **p)
