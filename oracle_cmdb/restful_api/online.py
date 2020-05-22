from collections import defaultdict

from restful_api import *
from .base import OraclePrivilegeReq
from auth.const import PRIVILEGE
from utils.schema_utils import *
from models.sqlalchemy import *
from ..tasks.capture.cmdb_task_capture import OracleCMDBTaskCapture
from ..statistics import *
from ..capture import *


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
            latest_task_record = OracleCMDBTaskCapture.\
                last_success_task_record_id_dict(
                    session, cmdb_id)
            latest_task_record_id = latest_task_record.get(cmdb_id, None)
            if not latest_task_record:
                return self.resp_bad_req(msg=f"当前库未采集或者没有采集成功。")

        tablespace_sum = {}
        stats_phy_size_object = OracleStatsCMDBPhySize.filter(
            task_record_id=latest_task_record_id,
            cmdb_id=cmdb_id
        ).first()
        if stats_phy_size_object:
            tablespace_sum = stats_phy_size_object.to_dict(
                iter_if=lambda k, v: k in ("total", "used", "usage_ratio", "free"),
                iter_by=lambda k, v: round(v, 2) if k in ("usage_ratio",) else v)

        sql_num = {}
        cmdb_sql_num = OracleStatsCMDBSQLNum.filter(
            target_login_user=self.current_user,
            cmdb_id=cmdb_id,
            task_record_id=latest_task_record_id,
            date_period=period
        ).first()
        if cmdb_sql_num:
            sql_num = cmdb_sql_num.to_dict(
                iter_if=lambda k, v: k in ("active", "at_risk"),
            )

        sql_execution_cost_rank = {'elapsed_time_total': [], 'elapsed_time_delta': []}
        sql_exec_cost_rank_q = OracleStatsCMDBSQLExecutionCostRank.filter(
            target_login_user=self.current_user,
            cmdb_id=cmdb_id,
            task_record_id=latest_task_record_id
        )
        if sql_exec_cost_rank_q:
            sql_execution_cost_rank['elapsed_time_total'].extend(
                [x.to_dict(iter_if=lambda k, v: k in ("sql_id", "time")) for x in
                 sql_exec_cost_rank_q.filter(by_what='elapsed_time_total')])
            sql_execution_cost_rank['elapsed_time_delta'].extend(
                [y.to_dict(iter_if=lambda k, v: k in ("sql_id", "time")) for y in
                 sql_exec_cost_rank_q.filter(by_what='elapsed_time_delta')])

        risk_rule_rank = []
        risk_rule_rank_d = defaultdict(lambda: {'issue_num': 0})
        risk_rule_rank_q = OracleStatsSchemaRiskRule.filter(
            cmdb_id=cmdb_id,
            task_record_id=latest_task_record_id
        )
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

    get.argument = {
        "querystring": {
            "cmdb_id": "2526",
            "//period": "7",
        },
        "json": {}
    }


@as_view("metadata", group="online")
class MetadataListHandler(OraclePrivilegeReq):

    def get(self):
        """元数据查询"""

        self.acquire(PRIVILEGE.PRIVILEGE_METADATA)

        params = self.get_query_args(Schema({
            "cmdb_id": scm_int,
            "schema_name": scm_unempty_str,
            scm_optional("table_name"): scm_unempty_str,
            scm_optional("search_type", default="exact"): And(
                scm_str, scm_one_of_choices(("icontains", "exact"))),
            **self.gen_p()
        }))
        cmdb_id = params.pop('cmdb_id')
        search_type = params.pop('search_type')
        if params.get("table_name", None):
            if "." in params["table_name"]:
                print(f"warning: the original table_name is {params['table_name']} "
                      f"the word before the dot is recognized as schema and has been ignored.")
                params["table_name"] = params["table_name"].split(".")[1]
            params[f"table_name__{search_type}"] = params.pop('table_name')
        p = self.pop_p(params)

        with make_session() as session:
            latest_task_record = OracleCMDBTaskCapture.last_success_task_record_id_dict(session, cmdb_id)
            latest_task_record_id = latest_task_record.get(cmdb_id, None)
            if not latest_task_record:
                return self.resp_bad_req(msg=f"当前库未采集或者没有采集成功。")

        tabinfo_q = OracleObjTabInfo.filter(
            cmdb_id=cmdb_id,
            task_record_id=latest_task_record_id
        ).filter(**params).order_by('-create_time')

        items, p = self.paginate(tabinfo_q, **p)
        self.resp([i.to_dict() for i in items], **p)

    get.argument = {
        "querystring": {
            "cmdb_id": "2526",
            "schema_name": "APEX",
            "//table_name": "PY_GVSQL",
            "//search_type": "icontains",
            "page": "1",
            "per_page": "10"
        },
        "json": {}
    }


@as_view("table_info", group="online")
class TableInfoHandler(OraclePrivilegeReq):

    def get(self):
        """表、表字段、表分区、表索引、表视图信息,库schema最新采集"""
        params = self.get_query_args(Schema({
            "cmdb_id": scm_int,
            "schema_name": scm_unempty_str,
            "table_name": scm_unempty_str
        }))
        if params.get("table_name", None) and "." in params["table_name"]:
            print(f"warning: the original table_name is {params['table_name']} "
                  f"the word before the dot is recognized as schema and has been ignored.")
            params["table_name"] = params["table_name"].split(".")[1]

        latest_tabinfo = OracleObjTabInfo.filter(table_name=params["table_name"]). \
            order_by("-create_time").first()
        if not latest_tabinfo:
            self.resp({}, msg="无数据。")
            return
        params["task_record_id"] = latest_tabinfo.task_record_id

        self.resp({
            'table': [i.to_dict(iter_if=lambda k, v: k in (
                "schema_name", "table_name", "table_type", "iot_name", "num_rows",
                "blocks", "avg_row_len", "last_analyzed", "last_ddl_time",
                "chain_cnt", "hwm_stat", "compression", "phy_size_mb"
            )) for i in OracleObjTabInfo.filter(**params)],

            'field': [i.to_dict(iter_if=lambda k, v: k in (
                "schema_name", "table_name", "column_name", "data_type", "nullable",
                "num_nulls", "num_distinct", "data_default", "avg_col_len"
            )) for i in OracleObjTabCol.filter(**params)],

            'partition': [i.to_dict(iter_if=lambda k, v: k in (
                "schema_name", "table_name", "partitioning_type", "column_name",
                "partitioning_key_count", "partition_count",
                "sub_partitioning_key_count", "sub_partitioning_type",
                "last_ddl_time", "phy_size_mb"
            )) for i in OracleObjPartTabParent.filter(**params)],  # "num_rows"TODO

            'index': [i.to_dict(iter_if=lambda k, v: k in (
                "schema_name", "index_name", "table_owner", "table_name", "column_name",
                "column_position", "descend"
            )) for i in OracleObjIndColInfo.filter(**params)],
            'view': [i.to_dict(iter_if=lambda k, v: k in (
                "obj_pk", "schema_name", "view_name", "referenced_owner", "referenced_name",
                "referenced_type"
            )) for i in OracleObjViewInfo.filter(**params)]
        })

    get.argument = {
        "querystring": {
            "cmdb_id": 2526,
            "schema_name": "IDBAAS",
            "table_name": "T_CMDB_ORACLE"
        },
        "json": {}
    }
