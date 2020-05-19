
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
            latest_task_record=OracleCMDBTaskCapture.last_success_task_record_id_dict(session,cmdb_id)
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
        cmdb_sql_num=OracleStatsCMDBSQLNum.objects(target_login_user=self.current_user,
                                      cmdb_id=cmdb_id,
                                      task_record_id=latest_task_record_id,
                                      date_period=period).first()
        if cmdb_sql_num:
            sql_num=cmdb_sql_num.to_dict(
                iter_if=lambda k, v: k in ("active", "at_risk"),
                )

        sql_execution_cost_rank={'elapsed_time_total':[],'elapsed_time_delta':[]}
        sql_exec_cost_rank_q=OracleStatsCMDBSQLExecutionCostRank.objects(target_login_user=self.current_user,
                                      cmdb_id=cmdb_id,
                                      task_record_id=latest_task_record_id)
        if sql_exec_cost_rank_q:
            sql_execution_cost_rank['elapsed_time_total'].extend([x.to_dict(iter_if=lambda k, v: k in ("sql_id", "time")) for x in sql_exec_cost_rank_q.filter(by_what='elapsed_time_total')])
            sql_execution_cost_rank['elapsed_time_delta'].extend([y.to_dict(iter_if=lambda k, v: k in ("sql_id", "time")) for y in sql_exec_cost_rank_q.filter(by_what='elapsed_time_delta')])

        risk_rule_rank=[]
        risk_rule_rank_d=defaultdict(lambda :{'issue_num':0})
        risk_rule_rank_q=OracleStatsSchemaRiskRule.objects(cmdb_id=cmdb_id,task_record_id=latest_task_record_id)
        for x in risk_rule_rank_q:
            doc = risk_rule_rank_d[x.rule['desc']]
            doc['rule'] = x.rule,
            doc['level'] = x.level,
            doc['issue_num'] += x.issue_num
        for x,y in risk_rule_rank_d.items():
            if y['issue_num']==0:
                continue
            risk_rule_rank.append(y)
        risk_rule_rank=sorted(risk_rule_rank, key=lambda x: (x["level"], -x['issue_num']))

        self.resp({
            # 以下是取最近一次采集分析的结果
            "tablespace_sum": tablespace_sum,
            "sql_num": sql_num,
            "sql_execution_cost_rank":sql_execution_cost_rank,
            "risk_rule_rank":risk_rule_rank,

            "cmdb_id":cmdb_id,
            "task_record_id":latest_task_record_id,
        })
