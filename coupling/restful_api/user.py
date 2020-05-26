import auth.restful_api.user
from utils.schema_utils import *
from auth.const import PRIVILEGE
from restful_api.modules import as_view
from models.sqlalchemy import *
from auth.user import *
from oracle_cmdb.auth.role import RoleOracleCMDBSchema
from oracle_cmdb.cmdb import *


@as_view(group="auth")
class OracleRelatedUserHandler(auth.restful_api.user.UserHandler):

    def get(self):
        """与oracle相关的平台登录用户列表"""
        params = self.get_query_args(Schema({
            scm_optional("has_privilege", default=None): And(
                scm_dot_split_str,
                scm_subset_of_choices(PRIVILEGE.get_all_privilege_id())
            ),
            **self.gen_p()
        }))
        p = self.pop_p(params)
        has_privilege = params.pop("has_privilege")
        with make_session() as session:
            user_q = self.filter_user(session, has_privilege)
            items, p = self.paginate(user_q, **p)
            to_ret = [i.to_dict() for i in items]

            # 给to_ret每个用户加上绑定的角色列表（包含角色id和角色名），
            #  以及纳管库的信息列表（connect_name, cmdb_id）
            for x in to_ret:
                keys = QueryEntity(
                    UserRole.role_id,
                    Role.role_name,
                    User.login_user
                )
                user_role = session.query(*keys). \
                    join(Role, UserRole.role_id == Role.role_id). \
                    join(User, UserRole.login_user == User.login_user)

                user_role = [list(x) for x in user_role]
                user_role = [y for y in user_role if x['login_user'] in y]

                x['role'] = [{'role_id': a[0], 'role_name': a[1]} for a in user_role]

                qe = QueryEntity(OracleCMDB.connect_name,
                                 OracleCMDB.cmdb_id,
                                 RoleOracleCMDBSchema.schema_name,
                                 RoleOracleCMDBSchema.create_time,
                                 RoleOracleCMDBSchema.comments,
                                 Role.role_name,
                                 Role.role_id)
                role_cmdb = session.query(*qe). \
                    join(OracleCMDB, RoleOracleCMDBSchema.cmdb_id == OracleCMDB.cmdb_id). \
                    join(Role, Role.role_id == RoleOracleCMDBSchema.role_id)

                role_cmdb = [list(x) for x in role_cmdb]
                role_cmdb = [b for b in role_cmdb for c in x['role'] if c['role_id'] in b]
                # 去重
                x['cmdbs'] = []
                connect_name = set()
                cmdb_id = set()
                for r_c in role_cmdb:
                    if r_c[0] in connect_name:
                        continue
                    if r_c[1] in cmdb_id:
                        continue
                    connect_name.add(r_c[0])
                    cmdb_id.add(r_c[1])
                    x['cmdbs'].append({"connect_name": r_c[0], "cmdb_id": r_c[1]})
            self.resp(to_ret, **p)

    get.argument = {
        "querystring": {
            "//has_privilege": "2,3,4,5,6,7,8",
            "page": "1",
            "per_page": "10"
        },
        "json": {}
    }
