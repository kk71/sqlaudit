# Author: kk.Fang(fkfkbill@gmail.com)

from typing import *
from collections import defaultdict

from .base import OraclePrivilegeReq
from ..cmdb import *
from ..auth.user_utils import *
from ..auth.role import RoleOracleCMDBSchema
from ..tasks.capture import OracleCMDBTaskCapture
from ..statistics.current_task.cmdb_score import OracleStatsCMDBScore
from utils import const
from utils.schema_utils import *
from utils.datetime_utils import *
from models.sqlalchemy import *
from restful_api.modules import as_view
from auth.user import Role
from auth.user import User,UserRole


@as_view("score_trend", group="cmdb")
class CMDBHealthTrendHandler(OraclePrivilegeReq):

    def post(self):
        """库健康评分趋势图,
        每一个评分为库的当天最后一次采集的分数,
        最近15天的库评分,
        折线图上展示的是15天内有数据的"""
        params = self.get_json_args(Schema({
            scm_optional("cmdb_id_list", default=()): list
        }), default_body="{}")
        cmdb_id_list :list = params.pop("cmdb_id_list")
        date_end = arrow.now().date()
        date_start = arrow.now().shift(weeks=-2).date()

        with make_session() as session:
            #如果没有cmdb_id_list，拿取用户纳管的cmdb_id
            if not cmdb_id_list:
                cmdb_q=self.cmdbs(session)
                cmdb_id_list =[cmdb.cmdb_id for cmdb in cmdb_q]

            cmdb_date_lastest_task_record = [] # 每一个库每天最后的任务id#[{"cmdb_id":[t1,t2]}]
            cmdb_id_connect = {}
            for cmdb_id in cmdb_id_list:
                cmdb_task = session.query(OracleCMDBTaskCapture).filter_by(cmdb_id=cmdb_id).first()
                date_latest_task_record :dict = cmdb_task.day_last_succeed_task_record_id(
                    date_start=date_start,date_end=date_end)
                cmdb_date_lastest_task_record.append({cmdb_task.cmdb_id:list(date_latest_task_record.values())})
                cmdb_id_connect[cmdb_task.cmdb_id] = cmdb_task.connect_name

            ret = defaultdict(dict)  # {"date":{"connect_name":"score"}}
            for cmdb_and_task_record_list in cmdb_date_lastest_task_record:
                cmdb_score_q=OracleStatsCMDBScore.filter(
                    cmdb_id=list(cmdb_and_task_record_list.keys()).pop(),
                    task_record_id__in=list(cmdb_and_task_record_list.values()).pop())
                for cmdb_score in cmdb_score_q:
                    cmdb_score.connect_name =  cmdb_id_connect[cmdb_score.cmdb_id]
                    ret[cmdb_score.create_time.date()][cmdb_score.connect_name] = \
                        cmdb_score.entry_score['ONLINE']
            ret = [{
                "date": d_to_str(k),
                **v
            } for k, v in ret.items()]


            base_lines = [
                i[0]
                for i in session.
                    query(OracleCMDB.baseline).
                    filter(OracleCMDB.cmdb_id.in_(cmdb_id_list)).
                    order_by(OracleCMDB.baseline)
            ]
            if not base_lines or base_lines[0] == 0:
                base_line = 80
            else:
                base_line = base_lines[0]

            self.resp({
                "data": ret,
                "fields": list(cmdb_id_connect.values()),
                "base_line": base_line
            })

    post.argument = {
        "json":{
            "cmdb_id_list":[2526,2529]
        }}


@as_view("oracle_cmdb", group="cmdb")
class CMDBHandler(OraclePrivilegeReq):

    def get(self):
        """oracle_cmdb列表,用于线上和健康中心请求"""

        params = self.get_query_args(Schema({
            # 精确匹配
            scm_optional("cmdb_id"): scm_gt0_int,
            scm_optional("connect_name"): scm_unempty_str,
            scm_optional("group_name"): scm_unempty_str,
            scm_optional("business_name"): scm_unempty_str,

            # 模糊匹配多个字段
            scm_optional("keyword", default=None): scm_str,

            # 排序
            scm_optional("sort", default=const.SORT_DESC): And(
                scm_str,
                self.scm_one_of_choices(const.ALL_SORTS)
            ),

            **self.gen_p()
        }))
        keyword = params.pop("keyword")
        sort = params.pop("sort")
        p = self.pop_p(params)

        with make_session() as session:
            cmdb_q=session.query(OracleCMDB).filter_by(**params)

            if keyword:
                cmdb_q = self.query_keyword(cmdb_q, keyword,
                                            OracleCMDB.cmdb_id,
                                            OracleCMDB.connect_name,
                                            OracleCMDB.group_name,
                                            OracleCMDB.business_name,
                                            OracleCMDB.server_name)

            # 构建输出的纳管库信息，带上评分
            all_current_cmdb: Dict[int, dict] = {}
            last_cmdb_score = OracleCMDBTaskCapture.last_cmdb_score(session)
            for a_cmdb in cmdb_q:
                all_current_cmdb[a_cmdb.cmdb_id] = {
                    "data_health": last_cmdb_score[a_cmdb.cmdb_id],
                    "rating_schemas": a_cmdb.rating_schemas(),
                    "rating_schemas_num": len(a_cmdb.rating_schemas()),
                    **a_cmdb.to_dict()
                }
            all_current_cmdb_list: List = list(all_current_cmdb.values())
            all_current_cmdb_list = sorted(
                all_current_cmdb_list,
                key=lambda k: k["data_health"].get("score", 0)
            )
            if sort == const.SORT_DESC:
                all_current_cmdb_list.reverse()
            ret, p = self.paginate(all_current_cmdb_list, **p)

            # 对分页之后的纳管库列表补充额外数据
            last_cmdb_task_record_id_dict = OracleCMDBTaskCapture. \
                last_login_user_entry_cmdb(
                    session,
                    self.current_user
                )
            for i in ret:
                i["stats"] = last_cmdb_task_record_id_dict[i["cmdb_id"]]

            # 给ret加上纳管它的角色信息（角色名，角色id）
            #  以及纳管它的用户(login_user, user_name)

            for i in ret:
                # 绑定当前库的角色
                cmdb_role = session.query(*(cmdb_role_qe := QueryEntity(
                    Role.role_name,
                    Role.role_id
                ))).filter(
                    Role.role_id == RoleOracleCMDBSchema.role_id,
                    RoleOracleCMDBSchema.cmdb_id == i["cmdb_id"]
                )
                i["role"] = [cmdb_role_qe.to_dict(i) for i in cmdb_role]
                role_ids = [i["role_id"] for i in i["role"]]

                # 绑定当前库的用户
                role_user = session.query(*(cmdb_user_qe := QueryEntity(
                    User.login_user,
                    User.username
                ))).filter(
                    UserRole.role_id.in_(role_ids),
                    UserRole.login_user == User.login_user)
                i['user'] = [cmdb_user_qe.to_dict(i) for i in role_user]
            self.resp(ret, **p)

    get.argument = {
        "querystring": {
            "//cmdb_id": 1,
            "//connect_name": "",
            "//group_name": "",
            "//business_name": "",
            "keyword": "",
            "sort": "desc",
            "page": 1,
            "per_page": 10
        }
    }
