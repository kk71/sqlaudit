# Author: kk.Fang(fkfkbill@gmail.com)

from typing import Dict
from collections import defaultdict

import task.const
import ticket.const
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
            managed_cmdb_by_group_name_verbose = self.dict_to_verbose_dict_in_list(
                managed_cmdb_by_group_name)

            # 纳管库采集任务的饼图
            qs, qe = OracleCMDBTaskCapture.query_cmdb_task_with_last_record(session)
            qs = qs.filter(OracleCMDBTaskCapture.cmdb_id.in_(self.cmdb_ids(session)))
            managed_cmdb_task_capture_by_status = defaultdict(lambda: 0)
            for a_cmdb_task in qs:
                a_cmdb_task_dict = qe.to_dict(a_cmdb_task)
                execution_status = a_cmdb_task_dict["execution_status"]
                if execution_status is None:
                    execution_status = task.const.TASK_NEVER_RAN
                execution_status_chinese = \
                    task.const.ALL_TASK_EXECUTION_STATUS_CHINESE_MAPPING[
                        execution_status]
                managed_cmdb_task_capture_by_status[execution_status_chinese] += 1
            managed_cmdb_task_capture_by_status_verbose = \
                self.dict_to_verbose_dict_in_list(managed_cmdb_task_capture_by_status)

            # 工单相关统计
            ticket_stats = OracleTicket.aggregate(
                {
                    "$match": {
                        "cmdb_id": {"$in": self.cmdb_ids(session)}
                    }
                },
                {
                    "$group": {
                        "_id": {
                            "status": "$status"
                        },
                        "count": {"$sum": 1}
                    }
                }
            )
            ticket_stats_dict: Dict[int, int] = {
                ticket.const.ALL_TICKET_STATUS_CHINESE[
                    [i["_id"]["status"]]]: i["count"]
                for i in ticket_stats
            }
            ticket_stats_list = self.dict_to_verbose_dict_in_list(ticket_stats_dict)

            ret = {
                "drill_down_sum": drill_down_sum_dict,
                "schema_rank": schema_rank_list,
                "tab_space_rank_list": tab_space_rank_list,
                "managed_cmdb_by_group_name": managed_cmdb_by_group_name_verbose,
                "managed_cmdb_task_capture_by_status_verbose": managed_cmdb_task_capture_by_status_verbose,
                "cmdb_num": self.cmdbs(session).count(),
                "ticket_stats_list": ticket_stats_list
            }
            self.resp(ret)

    get.argument = {
        "querystring": {}
    }
