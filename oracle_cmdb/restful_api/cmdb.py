# Author: kk.Fang(fkfkbill@gmail.com)

from typing import *
from collections import defaultdict

from .base import OraclePrivilegeReq
from ..cmdb import *
from ..auth.user_utils import *
from ..auth.role import RoleOracleCMDBSchema
from ..tasks.capture import OracleCMDBTaskCapture
from utils import const
from utils.schema_utils import *
from utils.datetime_utils import *
from models.sqlalchemy import *
from restful_api.modules import as_view
from auth.user import Role
from auth.user import User,UserRole
from auth.restful_api.base import AuthReq


@as_view("score_trend", group="cmdb")
class CMDBHealthTrendHandler(AuthReq):

    def post(self):
        """库健康评分趋势图"""
        params = self.get_json_args(Schema({
            scm_optional("cmdb_id_list", default=()): list
        }), default_body="{}")
        now = arrow.now()
        cmdb_id_list = params.pop("cmdb_id_list")

        with make_session() as session:
            if not cmdb_id_list:
                cmdb_id_list = current_cmdb(session, user_login=self.current_user)
                # 如果没有给出cmdb_id，则把最差的前十个拿出来
                cmdb_id_list = [
                                   i
                                   for i in oracle_latest_cmdb_score(session).keys()
                                   if i in cmdb_id_list
                               ][:10]
            fields = set()
            ret = defaultdict(dict)  # {date: [{health data}, ...]}
            for cmdb_id in cmdb_id_list:  # TODO stats
                dh_q = StatsCMDBRate.filter(
                    cmdb_id=cmdb_id,
                    etl_date__gt=now.shift(weeks=-2).datetime
                ).order_by("etl_date")
                for dh in dh_q:
                    ret[dh.etl_date.date()][dh.connect_name] = dh.score
                    fields.add(dh.connect_name)
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
            ret = [{
                "date": d_to_str(k),
                **v
            } for k, v in ret.items()]
            self.resp({
                "data": ret,
                "fields": list(fields),
                "base_line": base_line
            })


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
