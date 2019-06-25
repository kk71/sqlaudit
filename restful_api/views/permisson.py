# Author: kk.Fang(fkfkbill@gmail.com)

import cx_Oracle
from schema import Schema, Optional, And

from .base import AuthReq
from utils.schema_utils import *
from models.oracle import *
from utils.const import *
from utils import cmdb_utils


class RoleHandler(AuthReq):

    def get(self):
        """角色列表"""
        params = self.get_query_args(Schema({
            Optional("keyword", default=None): scm_str,
            **self.gen_p()
        }))
        keyword = params.pop("keyword")
        p = self.pop_p(params)
        with make_session() as session:
            role_q = session.query(Role)
            if keyword:
                role_q = self.query_keyword(role_q, keyword,
                                                 Role.role_id,
                                                 Role.role_name,
                                                 Role.comments)
            items, p = self.paginate(role_q, **p)
            ret = []
            for i in items:
                r = i.to_dict()
                r.update({
                    "privileges": [PRIVILEGE.privilege_to_dict(
                        PRIVILEGE.get_privilege_by_id(j.privilege_id)
                    ) for j in session.query(RolePrivilege).filter_by(role_id=r["role_id"])]
                })
                ret.append(r)
            self.resp(ret, **p)

    def post(self):
        """增加角色"""
        params = self.get_json_args(Schema({
            "role_name": scm_unempty_str,
            "comments": scm_str,
            "privileges": [scm_one_of_choices(PRIVILEGE.get_all_privilege_id())]
        }))
        privileges = [PRIVILEGE.privilege_to_dict(PRIVILEGE.get_privilege_by_id(i))
                      for i in params.pop("privileges")]
        with make_session() as session:
            if session.query(Role).filter_by(role_name=params["role_name"]).count():
                self.resp_forbidden(msg="已经存在该角色")
                return
            role = Role(**params)
            session.add(role)
            session.commit()
            session.refresh(role)
            session.bulk_save_objects([RolePrivilege(
                role_id=role.role_id,
                privilege_id=i["id"],
                privilege_type=i["type"]
            ) for i in privileges])
            self.resp_created({
                **role.to_dict(),
                "privileges": privileges
            })

    def patch(self):
        """编辑角色"""
        params = self.get_json_args(Schema({
            "role_id": scm_int,

            Optional("role_name"): scm_unempty_str,
            Optional("comments"): scm_str,
            Optional("privileges", default=None): [
                scm_one_of_choices(PRIVILEGE.get_all_privilege_id())]
        }))
        role_id = params.pop("role_id")
        privileges = params.pop("privileges")
        if privileges:
            privileges = [PRIVILEGE.privilege_to_dict(PRIVILEGE.get_privilege_by_id(i))
                          for i in privileges]
        with make_session() as session:
            role = session.query(Role).filter_by(role_id=role_id).first()
            if not role:
                return self.resp_not_found(msg="role not found.")
            role.from_dict(params)
            session.add(role)
            session.commit()
            session.refresh(role)
            if privileges:
                session.query(RolePrivilege).filter(RolePrivilege.role_id == role_id).delete()
                session.commit()
                session.add_all([RolePrivilege(
                    role_id=role.role_id,
                    privilege_id=i["id"],
                    privilege_type=i["type"]
                ) for i in privileges])
                session.commit()
        self.resp_created(msg="finished.")

    def delete(self):
        """删除角色"""
        params = self.get_json_args(Schema({
            "role_id": scm_int,
        }))
        with make_session() as session:
            session.query(RolePrivilege).filter_by(**params).delete()
            session.query(UserRole).filter_by(**params).delete()
            session.query(Role).filter_by(**params).delete()
        self.resp_created(msg="删除成功")


class RoleUserHandler(AuthReq):

    def get(self):
        """获取用户角色信息"""
        params = self.get_query_args(Schema({
            Optional("role_id", default=None): scm_int,
            Optional("login_user", default=None): scm_unempty_str,
            **self.gen_p()
        }))
        p = self.pop_p(params)
        with make_session() as session:
            keys = QueryEntity(
                User.user_name,
                UserRole.role_id,
                Role.role_name,
                User.login_user
            )
            user_role = session.query(*keys).\
                join(Role, UserRole.role_id == Role.role_id).\
                join(User, UserRole.login_user == User.login_user)
            items, p = self.paginate(user_role, **p)
            self.resp([keys.to_dict(i) for i in items])

    def post(self):
        """用户绑定角色"""
        params = self.get_json_args(Schema({
            "role_id": scm_int,
            "login_user": scm_unempty_str,
        }))
        with make_session() as session:
            ur = UserRole(**params)
            session.add(ur)
            session.commit()
            session.refresh(ur)
            self.resp_created(ur.to_dict())

    def delete(self):
        """用户取消角色"""
        params = self.get_json_args(Schema({
            "role_id": scm_int,
            "login_user": scm_unempty_str,
        }))
        with make_session() as session:
            session.query(UserRole).filter_by(**params).delete()
        self.resp_created(msg="deleted")


class SystemPrivilegeHandler(AuthReq):

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
            with make_session() as session:
                privilege_ids = [i[0] for i in session.query(RolePrivilege.privilege_id).
                    join(UserRole, RolePrivilege.role_id == UserRole.role_id).
                    filter(UserRole.login_user == self.current_user)]
            privilege_dicts = [PRIVILEGE.privilege_to_dict(PRIVILEGE.get_privilege_by_id(i))
                               for i in privilege_ids]
            privilege_dicts = [i["name"] for i in privilege_dicts if i["type"] in privilege_type]

        else:
            privilege_dicts = [PRIVILEGE.privilege_to_dict(i)
                               for i in PRIVILEGE.get_privilege_by_type(privilege_type)]

        items, p = self.paginate(privilege_dicts, **p)
        self.resp([i for i in items], **p)


class CMDBPermissionHandler(AuthReq):
    """数据库权限配置"""

    def get(self):
        params = self.get_query_args(Schema({
            **self.gen_p()
        }))
        p = self.pop_p(params)
        with make_session() as session:
            qe = QueryEntity(CMDB.connect_name,
                             CMDB.cmdb_id,
                             DataPrivilege.schema_name,
                             DataPrivilege.create_date,
                             DataPrivilege.comments,
                             User.user_name,
                             User.login_user)
            perm_datas = session.query(*qe). \
                join(CMDB, DataPrivilege.cmdb_id == CMDB.cmdb_id). \
                join(User, User.login_user == DataPrivilege.login_user)
            items, p = self.paginate(perm_datas, **p)
            self.resp([qe.to_dict(i) for i in perm_datas], **p)

    def patch(self):
        params = self.get_json_args(Schema({
            "cmdb_id": scm_int,
            "login_user": scm_unempty_str,
            "schema_names": [scm_unempty_str]
        }))
        login_user = params.pop("login_user")
        cmdb_id = params.pop("cmdb_id")
        schema_names: list = params.pop("schema_names")
        del params
        with make_session() as session:
            cmdb = session.query(CMDB).filter_by(cmdb_id=cmdb_id).first()
            try:
                available_schema: set = set(cmdb_utils.get_cmdb_available_schemas(cmdb))
            except cx_Oracle.DatabaseError as e:
                return self.resp(msg=f"获取可用的schema失败：{str(e)}")
            unavailable_schemas: set = set(schema_names) - available_schema
            if unavailable_schemas:
                print(available_schema)
                return self.resp_bad_req(msg=f"包含了无效的schema: {unavailable_schemas}")
            session.query(DataPrivilege).\
                filter(
                    DataPrivilege.login_user == login_user,
                    DataPrivilege.cmdb_id == cmdb_id).\
                delete(synchronize_session='fetch')
            session.add_all([DataPrivilege(
                cmdb_id=cmdb_id,
                login_user=login_user,
                schema_name=i
            ) for i in schema_names])
        return self.resp_created(msg="分配权限成功")

    def delete(self):
        params = self.get_json_args(Schema({
            'cmdb_id': scm_int,
            'login_user': scm_unempty_str,
            'schema_name': scm_unempty_str
        }))
        with make_session() as session:
            session.query(DataPrivilege).filter_by(**params).delete()
        self.resp_created(msg="删除成功")
