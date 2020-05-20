from collections import defaultdict

from restful_api.modules import as_view
from .base import OraclePrivilegeReq
from auth.const import PRIVILEGE
from utils.schema_utils import *
from models.sqlalchemy import make_session
from ..statistics.login_user_current import OracleStatsCMDBSQLNum
from ..tasks.capture.cmdb_task_capture import OracleCMDBTaskCapture
from ..statistics.current_task.phy_size import OracleStatsCMDBPhySize
from ..statistics.login_user_current_rank.sql_execution_cost_rank import OracleStatsCMDBSQLExecutionCostRank
from ..statistics.current_task.risk_rule import OracleStatsSchemaRiskRule
from ..capture.obj_tab_space import OracleObjTabSpace


@as_view("overview", group="online")
class OverviewHandler(OraclePrivilegeReq):

    def get(self):
        """数据库健康度概览"""
        self.acquire(PRIVILEGE.PRIVILEGE_ONLINE)

        params = self.get_query_args(Schema({
            "cmdb_id": scm_int,

            scm_optional("period", default=OracleStatsCMDBSQLNum.DATE_PERIOD[0]):
                And(scm_int, scm_one_of_choices(OracleStatsCMDBSQLNum.DATE_PERIOD))
        }))
        cmdb_id = params.pop("cmdb_id")
        period = params.pop("period")
        del params  # shouldn't use params anymore

        with make_session() as session:
            latest_task_record = OracleCMDBTaskCapture.last_success_task_record_id_dict(session, cmdb_id)
            latest_task_record_id = latest_task_record.get(cmdb_id, None)
            if not latest_task_record:
                return self.resp_bad_req(msg=f"当前库未采集或者没有采集成功。")

        tablespace_sum = {}
        stats_phy_size_object = OracleStatsCMDBPhySize.objects(
            task_record_id=latest_task_record_id, cmdb_id=cmdb_id).first()
        if stats_phy_size_object:
            tablespace_sum = stats_phy_size_object.to_dict(
                iter_if=lambda k, v: k in ("total", "used", "usage_ratio", "free"),
                iter_by=lambda k, v: round(v, 2) if k in ("usage_ratio",) else v)

        sql_num = {}
        cmdb_sql_num = OracleStatsCMDBSQLNum.objects(target_login_user=self.current_user,
                                                     cmdb_id=cmdb_id,
                                                     task_record_id=latest_task_record_id,
                                                     date_period=period).first()
        if cmdb_sql_num:
            sql_num = cmdb_sql_num.to_dict(
                iter_if=lambda k, v: k in ("active", "at_risk"),
            )

        sql_execution_cost_rank = {'elapsed_time_total': [], 'elapsed_time_delta': []}
        sql_exec_cost_rank_q = OracleStatsCMDBSQLExecutionCostRank.objects(target_login_user=self.current_user,
                                                                           cmdb_id=cmdb_id,
                                                                           task_record_id=latest_task_record_id)
        if sql_exec_cost_rank_q:
            sql_execution_cost_rank['elapsed_time_total'].extend(
                [x.to_dict(iter_if=lambda k, v: k in ("sql_id", "time")) for x in
                 sql_exec_cost_rank_q.filter(by_what='elapsed_time_total')])
            sql_execution_cost_rank['elapsed_time_delta'].extend(
                [y.to_dict(iter_if=lambda k, v: k in ("sql_id", "time")) for y in
                 sql_exec_cost_rank_q.filter(by_what='elapsed_time_delta')])

        risk_rule_rank = []
        risk_rule_rank_d = defaultdict(lambda: {'issue_num': 0})
        risk_rule_rank_q = OracleStatsSchemaRiskRule.objects(cmdb_id=cmdb_id, task_record_id=latest_task_record_id)
        for x in risk_rule_rank_q:
            doc = risk_rule_rank_d[x.rule['desc']]
            doc['rule'] = x.rule,
            doc['level'] = x.level,
            doc['issue_num'] += x.issue_num
        for x, y in risk_rule_rank_d.items():
            if y['issue_num'] == 0:
                continue
            risk_rule_rank.append(y)
        risk_rule_rank = sorted(risk_rule_rank, key=lambda x: (x["level"], -x['issue_num']))

        self.resp({
            # 以下是取最近一次采集分析的结果
            "tablespace_sum": tablespace_sum,
            "sql_num": sql_num,
            "sql_execution_cost_rank": sql_execution_cost_rank,
            "risk_rule_rank": risk_rule_rank,

            "cmdb_id": cmdb_id,
            "task_record_id": latest_task_record_id,
        })


@as_view("tabspace_list", group="online")
class TablespaceListHandler(OraclePrivilegeReq):

    def get(self):
        """表空间列表,库的最后一次采集"""
        params = self.get_query_args(Schema({
            "cmdb_id": scm_gt0_int,
            **self.gen_p()
        }))
        p = self.pop_p(params)
        cmdb_id = params.pop("cmdb_id")

        with make_session() as session:
            latest_task_record = OracleCMDBTaskCapture.last_success_task_record_id_dict(session, cmdb_id)
            latest_task_record_id = latest_task_record.get(cmdb_id, None)
            if not latest_task_record:
                return self.resp_bad_req(msg=f"当前库未采集或者没有采集成功。")

        tabspace_q = OracleObjTabSpace.objects(cmdb_id=cmdb_id,
                                               task_record_id=latest_task_record_id).order_by("-usage_ratio")
        items, p = self.paginate(tabspace_q, **p)
        self.resp([i.to_dict() for i in items], **p)

    get.argument = {
        "querystring": {
            "cmdb_id": "2526",
            "page": '1',
            "per_page": '10'
        },
        "json": {}
    }


@as_view("tabspace_history", group="online")
class TablespaceHistoryHandler(OraclePrivilegeReq):

    def get(self):
        """某个表空间的使用率历史折线图"""
        params = self.get_query_args(Schema({
            "cmdb_id": scm_int,
            "tablespace_name": scm_unempty_str,
        }))
        tabspace_q = OracleObjTabSpace.objects(**params).order_by("-create_time").limit(30)
        ret = self.list_of_dict_to_date_axis(
            [x.to_dict(datetime_to_str=False) for x in tabspace_q],
            "create_time",
            "usage_ratio"
        )
        ret = self.dict_to_verbose_dict_in_list(dict(ret[-7:]))
        self.resp(ret)

    get.argument = {
        "querystring": {
            "cmdb_id": "2526",
            "tablespace_name": "SYSAUX",
        },
        "json": {}
    }


@as_view("tabspace_total_history", group="online")
class TablespaceTotalHistoryHandler(OraclePrivilegeReq):

    def get(self):
        """总表空间使用率历史折线图"""
        params = self.get_query_args(Schema({
            "cmdb_id": scm_int,
        }))
        phy_size_q = OracleStatsCMDBPhySize.objects(**params).order_by("-create_time").limit(30)

        ret = self.list_of_dict_to_date_axis(
            [i.to_dict(datetime_to_str=False) for i in phy_size_q],
            "create_time",
            "usage_ratio"
        )
        ret = self.dict_to_verbose_dict_in_list(dict(ret[-7:]))
        self.resp(ret)

    get.argument = {
        "querystring": {
            "cmdb_id": "2526",
        },
        "json": {}
    }
