# Author: kk.Fang(fkfkbill@gmail.com)

from schema import Schema

from models.sqlalchemy import make_session
from oracle_cmdb.capture import OracleObjTabSpace
from oracle_cmdb.restful_api.base import OraclePrivilegeReq
from oracle_cmdb.statistics import OracleStatsCMDBPhySize
from oracle_cmdb.tasks.capture import OracleCMDBTaskCapture
from restful_api import as_view
from utils.schema_utils import scm_gt0_int, scm_int, scm_unempty_str


@as_view("list", group="online")
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

        tabspace_q = OracleObjTabSpace.filter(
            cmdb_id=cmdb_id,
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


@as_view("history", group="online")
class TablespaceHistoryHandler(OraclePrivilegeReq):

    def get(self):
        """某个表空间的使用率历史折线图"""
        params = self.get_query_args(Schema({
            "cmdb_id": scm_int,
            "tablespace_name": scm_unempty_str,
        }))
        tabspace_q = OracleObjTabSpace.filter(
            **params).order_by("-create_time").limit(30)
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


@as_view("total_history", group="online")
class TablespaceTotalHistoryHandler(OraclePrivilegeReq):

    def get(self):
        """总表空间使用率历史折线图"""
        params = self.get_query_args(Schema({
            "cmdb_id": scm_int,
        }))
        phy_size_q = OracleStatsCMDBPhySize.filter(
            **params).order_by("-create_time").limit(30)

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
