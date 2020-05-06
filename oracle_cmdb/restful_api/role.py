# Author: kk.Fang(fkfkbill@gmail.com)

from schema import Or

from restful_api.modules import *
from models.sqlalchemy import *
from auth.restful_api.base import *
from auth.const import PRIVILEGE
from utils.schema_utils import *
from auth.user import *
from ..auth.role import RoleOracleCMDBSchema
from ..cmdb import *


@as_view("cmdb_schema", group="oracle_role")
class RoleCMDBSchemaRelationHandler(PrivilegeReq):

    def get(self):
        """数据权限：角色-oracle库-schema的绑定关系查询"""

        self.acquire(PRIVILEGE.PRIVILEGE_ROLE_DATA_PRIVILEGE)

        params = self.get_query_args(Schema({
            # 精准过滤
            scm_optional("role_id", default=None): scm_gt0_int,
            scm_optional("cmdb_id", default=None): scm_gt0_int,

            # 模糊搜索
            scm_optional("keyword", default=None): scm_str,

            **self.gen_p()
        }))
        p = self.pop_p(params)
        role_id = params.pop("role_id")
        cmdb_id = params.pop("cmdb_id")
        keyword = params.pop("keyword")
        del params
        with make_session() as session:
            qe = QueryEntity(OracleCMDB.connect_name,
                             OracleCMDB.cmdb_id,
                             RoleOracleCMDBSchema.schema_name,
                             RoleOracleCMDBSchema.create_time,
                             RoleOracleCMDBSchema.comments,
                             Role.role_name,
                             Role.role_id)
            perm_datas = session.query(*qe). \
                join(OracleCMDB, RoleOracleCMDBSchema.cmdb_id == OracleCMDB.cmdb_id). \
                join(Role, Role.role_id == RoleOracleCMDBSchema.role_id)
            if keyword:
                perm_datas = self.query_keyword(perm_datas, keyword,
                                                Role.role_name,
                                                OracleCMDB.connect_name,
                                                RoleOracleCMDBSchema.schema_name)
            if role_id:
                perm_datas = perm_datas.filter(RoleOracleCMDBSchema.role_id == role_id)
            if cmdb_id:
                perm_datas = perm_datas.filter(OracleCMDB.cmdb_id == cmdb_id)
            items, p = self.paginate(perm_datas, **p)
            self.resp([qe.to_dict(i) for i in perm_datas], **p)

    def patch(self):
        """数据权限：角色-oracle库-schema的绑定关系修改"""
        params = self.get_json_args(Schema({
            "role_id": scm_gt0_int,
            "cmdbs": [
                {
                    "cmdb_id": scm_gt0_int,
                    scm_optional("schemas", default=[]): Or([scm_unempty_str], []),
                    scm_optional(object): object  # 兼容前端
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
                cmdb = session.query(OracleCMDB).filter_by(cmdb_id=cmdb_id).first()

                try:
                    available_schema: set = set(cmdb.get_available_schemas())
                except Exception as e:
                    session.rollback()
                    return self.resp(msg=f"获取可用的schema失败(cmdb_id: {cmdb_id})：{str(e)}")
                unavailable_schemas: set = set(schema_names) - available_schema
                if unavailable_schemas:
                    print(available_schema)
                    session.rollback()
                    return self.resp_bad_req(msg=f"包含了无效的schema: {unavailable_schemas}")
                old_rdps = session.query(RoleOracleCMDBSchema). \
                    filter(
                    RoleOracleCMDBSchema.role_id == role_id,
                    RoleOracleCMDBSchema.cmdb_id == cmdb_id)
                for old_rdp in old_rdps:
                    session.delete(old_rdp)
                session.add_all([RoleOracleCMDBSchema(
                    cmdb_id=cmdb_id,
                    role_id=role_id,
                    schema_name=i
                ) for i in schema_names])
                used_cmdb_id.add(cmdb_id)
        return self.resp_created(msg="分配权限成功")

    def delete(self):
        """数据权限：角色-oracle库-schema的绑定关系删除"""
        params = self.get_json_args(Schema({
            'cmdb_id': scm_int,
            'role_id': scm_int,
            scm_optional('schema_name', default=None): scm_unempty_str
        }))
        if not params["schema_name"]:
            params.pop("schema_name")
        with make_session() as session:
            session.query(RoleOracleCMDBSchema).filter_by(**params). \
                delete(synchronize_session=False)

        self.resp_created(msg="删除成功")

