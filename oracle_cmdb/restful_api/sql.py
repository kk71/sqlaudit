# Author: kk.Fang(fkfkbill@gmail.com)

from typing import List, Dict, Union
from collections import defaultdict

from utils.datetime_utils import *
from utils.schema_utils import *
from restful_api import *
from .base import *
from models.sqlalchemy import *
from models.mongoengine import *
from ..tasks.capture.cmdb_task_capture import OracleCMDBTaskCapture
from ..capture import OracleSQLStat, OracleSQLStatToday, OracleSQLStatYesterday
from ..statistics import OracleStatsSQLText, OracleStatsSchemaRiskSQL


@as_view(group="online")
class SQLHandler(OraclePrivilegeReq):

    # 需要取的字段
    ORIGINAL_KEYS = (
        "buffer_gets_delta",
        "cpu_time_delta",
        "disk_reads_delta",
        "elapsed_time_delta"
    )
    AVERAGE_POSTFIX = "_average"

    @staticmethod
    def prettify_coordinate(
            d: date,
            value: Union[int, float]) -> Dict[str, Union[float, str]]:
        return {
            "key": d_to_str(d),
            "value": round(value, 2)
        }

    def get(self):
        """线上审核的SQL详情页面，风险SQL详情"""

        now = arrow.now()
        params = self.get_query_args(Schema({
            "cmdb_id": scm_gt0_int,
            "sql_id": scm_unempty_str,
            scm_optional("rule_name", default=None): scm_empty_as_optional(scm_str),
            **self.gen_date(
                date_start=now.shift(weeks=-1).date(),
                date_end=now.date()
            )
        }))
        cmdb_id: int = params.pop("cmdb_id")
        sql_id: str = params.pop("sql_id")
        rule_name: str = params.pop("rule_name")
        date_start, date_end = self.pop_date(params)

        with make_session() as session:
            the_cmdb = self.cmdbs(session).filter_by(cmdb_id=cmdb_id).first()
            the_cmdb_task = OracleCMDBTaskCapture.get_cmdb_task_by_cmdb(the_cmdb)
            lstri: int = the_cmdb_task.last_success_task_record_id
            # 日期区间内当前库成功的task_record_id，以字典形式和列表形式
            lstri_in_date_period_dict: Dict[date, int] = \
                the_cmdb_task.day_last_succeed_task_record_id(
                    date_start=date_start, date_end=date_end, no_none=True)
            lstri_in_date_period: List[int] = list(lstri_in_date_period_dict.values())
            # SQL文本信息统计
            latest_sql_text_stats = OracleStatsSQLText.filter(
                task_record_id=lstri, sql_id=sql_id).first()
            # 取一个最后一次成功任务的创建时间最近的一个sql执行状态
            latest_sql_stat = OracleSQLStat.filter(
                task_record_id=lstri,
                sql_id=sql_id
            ).order_by("-create_time").first()

            # 时间段内触犯的风险规则信息
            dollar_match = {
                "task_record_id": {"$in": lstri_in_date_period},
                "sql_id": sql_id
            }
            if rule_name:
                dollar_match["rule_name"] = rule_name
            risk_rules = OracleStatsSchemaRiskSQL.aggregate(
                {
                    "$match": dollar_match
                },
                {
                    "$group": {
                        "_id": {
                            "rule_name": "$rule_name",
                            "rule_desc": "$rule_desc",
                            "level": "$level",
                            "rule_solution": "$rule_solution"
                        }
                    }
                }
            )
            risk_rules = [i["_id"] for i in risk_rules]

            # 求出所有当前SQL在给定时间区间内成功采集到的执行计划plan_hash_value
            plan_hash_values = OracleSQLStat.filter(
                task_record_id__in=lstri_in_date_period,
                sql_id=sql_id,
            ).distinct("plan_hash_value")

            # 执行计划列表
            plans = []
            for plan_hash_value in plan_hash_values:
                q = Q(
                    task_record_id__in=lstri_in_date_period,
                    sql_id=sql_id,
                    plan_hash_value=plan_hash_value
                )
                # 先尝试在完整的某一天里寻找该执行信息
                a_stat = OracleSQLStatYesterday.filter(q).order_by("-create_time").first()
                if not a_stat:
                    # 如果没找到，则去采集当日不完整的一天里寻找
                    a_stat = OracleSQLStatToday.filter(q).order_by("-create_time").first()
                plans.append(a_stat.to_dict())

            # 执行信息折线图
            # plan_hash_value: graph_perspective: [{point}, ...]
            graph = defaultdict(lambda: defaultdict(list))
            for plan_hash_value in plan_hash_values:

                # graph of current plan_hash_value
                gocphv: Dict[str, List[Dict[str, Union[str, float]]]] = \
                    graph[plan_hash_value]

                for d, the_task_record_id in lstri_in_date_period_dict.items():
                    q = Q(
                        task_record_id=the_task_record_id,
                        sql_id=sql_id,
                        plan_hash_value=plan_hash_value
                    )
                    a_stat = OracleSQLStatYesterday.filter(q).first()
                    if not a_stat:
                        a_stat = OracleSQLStatToday.filter(q).first()
                    if not a_stat:
                        continue
                    for k in self.ORIGINAL_KEYS:
                        # 总值
                        original_value = getattr(a_stat, k)
                        gocphv[k].append(
                            self.prettify_coordinate(d, original_value))
                        # 平均值
                        average_value = 0
                        if a_stat.executions_delta:
                            average_value = original_value / a_stat.executions_delta
                        gocphv[k + self.AVERAGE_POSTFIX].append(
                            self.prettify_coordinate(d, average_value))

        self.resp({
            "sql_stat": latest_sql_stat.to_dict(iter_if=lambda k, v: k in (
                "elapsed_time_delta", "executions_total", "io_cost"
            )),
            "graph": graph,
            "risk_rules": risk_rules,
            "plans": plans,
            **latest_sql_text_stats.to_dict()
        })

    get.argument = {
        "querystring": {
            "cmdb_id": 2526,
            "sql_id": "8x6q9fm2xfpmu",
            "//rule_name": "WHERE_FUNC",
            "//date_start": "时间范围可选，默认就是近来的7天",
            "//date_end": "",
        }
    }
