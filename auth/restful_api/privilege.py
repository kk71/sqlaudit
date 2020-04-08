# Author: kk.Fang(fkfkbill@gmail.com)
from schema import Or

from .base import PrivilegeReq
from auth.user import *

from utils.cmdb_utils import *
from utils.const import PRIVILEGE
from utils.schema_utils import *
from cmdb.cmdb import *
from oracle_cmdb.cmdb import RoleDataPrivilege
from models.sqlalchemy import make_session, QueryEntity
from restful_api.modules import as_view


@as_view("auth", group="auth")
class SystemPrivilegeHandler(PrivilegeReq):

    def get(self):
        """权限列表"""
        params = self.get_query_args(Schema({
            Optional("type", default=PRIVILEGE.ALL_PRIVILEGE_TYPE):
                And(scm_dot_split_int, scm_subset_of_choices(PRIVILEGE.ALL_PRIVILEGE_TYPE)),
            Optional("current_user", default=False): scm_bool,
            **self.gen_p(per_page=99)
        }))
        privilege_type: list = params.pop("type")
        current_user = params.pop("current_user")
        p = self.pop_p(params)

        if current_user:
            if self.is_admin():
                # admin用户拥有任何权限
                privilege_ids = PRIVILEGE.get_all_privilege_id()
            else:
                with make_session() as session:
                    privilege_ids = [i[0] for i in session.query(RolePrivilege.privilege_id).
                        join(UserRole, RolePrivilege.role_id == UserRole.role_id).
                        filter(UserRole.login_user == self.current_user)]
            privilege_dicts = [PRIVILEGE.privilege_to_dict(PRIVILEGE.get_privilege_by_id(i))
                               for i in privilege_ids if PRIVILEGE.get_privilege_by_id(i)]
            privilege_dicts = [i["name"] for i in privilege_dicts if i["type"] in privilege_type]

        else:
            privilege_dicts = [PRIVILEGE.privilege_to_dict(i)
                               for i in PRIVILEGE.get_privilege_by_type(privilege_type)]

        items, p = self.paginate(privilege_dicts, **p)
        self.resp([i for i in items], **p)


@as_view("auth", group="auth")
class CMDBPermissionHandler(PrivilegeReq):

    def get(self):
        """数据库权限配置"""

        self.acquire(PRIVILEGE.PRIVILEGE_ROLE_DATA_PRIVILEGE)

        params = self.get_query_args(Schema({
            # 精准过滤
            Optional("role_id", default=None): scm_gt0_int,
            Optional("cmdb_id", default=None): scm_gt0_int,

            # 模糊搜索
            Optional("keyword", default=None): scm_str,

            **self.gen_p()
        }))
        p = self.pop_p(params)
        role_id = params.pop("role_id")
        cmdb_id = params.pop("cmdb_id")
        keyword = params.pop("keyword")
        del params
        with make_session() as session:
            qe = QueryEntity(CMDB.connect_name,
                             CMDB.cmdb_id,
                             RoleDataPrivilege.schema_name,
                             RoleDataPrivilege.create_date,
                             RoleDataPrivilege.comments,
                             Role.role_name,
                             Role.role_id)
            perm_datas = session.query(*qe). \
                join(CMDB, RoleDataPrivilege.cmdb_id == CMDB.cmdb_id). \
                join(Role, Role.role_id == RoleDataPrivilege.role_id)
            if keyword:
                perm_datas = self.query_keyword(perm_datas, keyword,
                                                Role.role_name,
                                                CMDB.connect_name,
                                                RoleDataPrivilege.schema_name)
            if role_id:
                perm_datas = perm_datas.filter(RoleDataPrivilege.role_id == role_id)
            if cmdb_id:
                perm_datas = perm_datas.filter(CMDB.cmdb_id == cmdb_id)
            items, p = self.paginate(perm_datas, **p)
            self.resp([qe.to_dict(i) for i in perm_datas], **p)

    def patch(self):
        params = self.get_json_args(Schema({
            "role_id": scm_gt0_int,
            "cmdbs": [
                {
                    "cmdb_id": scm_gt0_int,
                    Optional("schemas", default=[]): Or([scm_unempty_str], []),
                    Optional(object): object  # 兼容前端
                }
            ]
        }))
        role_id = params.pop("role_id")
        cmdb_id_schemas: list = params.pop("cmdbs")
        del params
        with make_session() as session:
            used_cmdb_id: set = set()
            for a_cmdb_id_schemas in cmdb_id_schemas:
                cmdb_id = a_cmdb_id_schemas["cmdb_id"]
                schema_names = a_cmdb_id_schemas["schemas"]
                if cmdb_id in used_cmdb_id:
                    print(f"{cmdb_id} is duplicated and the first schemas list is"
                          f" used for binding, the remained are ignored.")
                    session.rollback()
                    return self.resp_bad_req(msg="cmdb_id={cmdb_id}存在重复项")
                cmdb = session.query(CMDB).filter_by(cmdb_id=cmdb_id).first()
                try:
                    available_schema: set = set(get_cmdb_available_schemas(cmdb))
                except Exception as e:
                    session.rollback()
                    return self.resp(msg=f"获取可用的schema失败(cmdb_id: {cmdb_id})：{str(e)}")
                unavailable_schemas: set = set(schema_names) - available_schema
                if unavailable_schemas:
                    print(available_schema)
                    session.rollback()
                    return self.resp_bad_req(msg=f"包含了无效的schema: {unavailable_schemas}")
                old_rdps = session.query(RoleDataPrivilege). \
                    filter(
                    RoleDataPrivilege.role_id == role_id,
                    RoleDataPrivilege.cmdb_id == cmdb_id)
                for old_rdp in old_rdps:
                    session.delete(old_rdp)
                session.add_all([RoleDataPrivilege(
                    cmdb_id=cmdb_id,
                    role_id=role_id,
                    schema_name=i
                ) for i in schema_names])
                used_cmdb_id.add(cmdb_id)
        return self.resp_created(msg="分配权限成功")

    def delete(self):
        params = self.get_json_args(Schema({
            'cmdb_id': scm_int,
            'role_id': scm_int,
            Optional('schema_name', default=None): scm_unempty_str
        }))
        if not params["schema_name"]:
            params.pop("schema_name")
        with make_session() as session:
            session.query(RoleDataPrivilege).filter_by(**params). \
                delete(synchronize_session=False)

        self.resp_created(msg="删除成功")

