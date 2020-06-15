# Author: kk.Fang(fkfkbill@gmail.com)

from collections import defaultdict

from models.sqlalchemy import *
from oracle_cmdb.restful_api.base import OraclePrivilegeReq
from oracle_cmdb.statistics import OracleStatsDashboardDrillDownSum
from oracle_cmdb.statistics import OracleStatsSchemaRank
from oracle_cmdb.statistics import OracleStatsTableSpaceRank
from oracle_cmdb.tasks.capture.cmdb_task_capture import *
from oracle_cmdb.ticket.ticket import OracleTicket
from restful_api import *


@as_view(group="dashboard")
class DashboardHandler(OraclePrivilegeReq):

    def get(self):
        """仪表盘数据"""

        with make_session() as session:
            # 所有平台的库的纳管库采集任务的最后一次成功的task_record_id
            last_cmdb_task_capture_task_record_id = OracleCMDBTaskCaptureRecord.\
                last_success_task_record_id(session)

            # 当前登录用户纳管的所有库的cmdb_id和connect_name的映射
            connect_name_cmdb_id_mapping = {
                i.cmdb_id: i.connect_name
                for i in self.cmdbs(session)
            }

            # 顶部四个下钻入口
            drill_down_sum_q = OracleStatsDashboardDrillDownSum.filter(
                task_record_id=last_cmdb_task_capture_task_record_id,
                target_login_user=self.current_user
            )
            drill_down_sum_dict = {i.entry: i.to_dict() for i in drill_down_sum_q}

            # 当前登录用户的所有纳管库的所有schema的排名
            schema_rank_q = OracleStatsSchemaRank.filter(
                task_record_id=last_cmdb_task_capture_task_record_id,
                target_login_user=self.current_user
            )
            schema_rank_list = [
                {
                    "schema_name": i.target_schema_name,
                    "connect_name": connect_name_cmdb_id_mapping[i.target_cmdb_id],
                    "health_score": i.score
                } for i in schema_rank_q
            ]

            # 纳管的库的表空间排名
            tab_space_rank_q = OracleStatsTableSpaceRank.filter(
                task_record_id=last_cmdb_task_capture_task_record_id,
                target_login_user=self.current_user
            )
            tab_space_rank_list = [
                {
                    "connect_name": connect_name_cmdb_id_mapping[i.target_cmdb_id],
                    **i.to_dict(iter_if=lambda k, v: k in (
                        "usage_ratio", "tablespace_name"))
                } for i in tab_space_rank_q
            ]

            # 纳管的库数，按照group_name分组
            managed_cmdb_by_group_name = defaultdict(lambda: 0)
            for a_cmdb in self.cmdbs(session):
                managed_cmdb_by_group_name[a_cmdb.group_name] += 1

            # 工单相关统计
            ticket_stats_dict = OracleTicket.num_stats(self.cmdb_ids(session))

            ret = {
                "drill_down_sum": drill_down_sum_dict,
                "schema_rank": schema_rank_list,
                "tab_space_rank_list": tab_space_rank_list,
                "managed_cmdb_by_group_name":
                    self.dict_to_verbose_dict_in_list(
                        managed_cmdb_by_group_name),
                "cmdb_num": self.cmdbs(session).count(),
                "ticket_stats_list": self.dict_to_verbose_dict_in_list(
                    ticket_stats_dict)
            }
            self.resp(ret)

    get.argument = {
        "querystring": {}
    }


@as_view("task", group="dashboard")
class DashboardTaskHandler(OraclePrivilegeReq):

    def get(self):
        """仪表盘Task,
        用户纳管库采集任务状态饼图"""
        with make_session() as session:
            managed_cmdb_task_capture_by_status = OracleCMDBTaskCapture. \
                num_stats_by_execution_status(
                session,
                self.cmdb_ids(session)
            )
            self.resp(managed_cmdb_task_capture_by_status)

    get.argument = {
        "querystring": {}
    }
