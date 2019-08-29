# Author: kk.Fang(fkfkbill@gmail.com)

import cx_Oracle
from schema import Schema, Optional, And

from .base import AuthReq, PrivilegeReq
from utils.schema_utils import *
from models.oracle import *
from utils.const import *
from utils import cmdb_utils
from task.clear_cache import clear_cache
from sqlalchemy.exc import IntegrityError


class RoleHandler(PrivilegeReq):

    def get(self):
        """角色列表"""

        self.acquire(PRIVILEGE.PRIVILEGE_ROLE_MANAGE)

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
                    ) for j in session.query(RolePrivilege).filter_by(role_id=r["role_id"])
                        if PRIVILEGE.get_privilege_by_id(j.privilege_id)]
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
                      for i in params.pop("privileges") if PRIVILEGE.get_privilege_by_id(i)]
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
                          for i in privileges if PRIVILEGE.get_privilege_by_id(i)]
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


class RoleUserHandler(PrivilegeReq):

    def get(self):
        """获取用户角色信息"""

        self.acquire(PRIVILEGE.PRIVILEGE_ROLE_USER_MANAGE)

        params = self.get_query_args(Schema({
            Optional("role_id", default=None): scm_int,
            Optional("login_user", default=None): scm_unempty_str,
            Optional("keyword", default=None): scm_str,
            **self.gen_p()
        }))
        p = self.pop_p(params)
        keyword = params.pop("keyword")

        with make_session() as session:
            keys = QueryEntity(
                User.user_name,
                UserRole.role_id,
                Role.role_name,
                User.login_user
            )
            user_role = session.query(*keys). \
                join(Role, UserRole.role_id == Role.role_id). \
                join(User, UserRole.login_user == User.login_user)
            if keyword:
                user_role = self.query_keyword(user_role, keyword,
                                               User.user_name,
                                               Role.role_name)
            items, p = self.paginate(user_role, **p)
            self.resp([keys.to_dict(i) for i in items])

    def post(self):
        """用户绑定角色"""
        params = self.get_json_args(Schema({
            "role_id": scm_int,
            "login_user": scm_unempty_str,
        }))
        with make_session() as session:
            try:
                ur = UserRole(**params)
                session.add(ur)
                session.commit()
                session.refresh(ur)
            except IntegrityError as e:
                return self.resp_bad_req(msg="角色已经绑定")
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


class CMDBPermissionHandler(PrivilegeReq):

    def get(self):
        """数据库权限配置"""

        self.acquire(PRIVILEGE.PRIVILEGE_ROLE_DATA_PRIVILEGE)

        params = self.get_query_args(Schema({
            **self.gen_p(),
            Optional("keyword", default=None): scm_str
        }))
        p = self.pop_p(params)
        keyword = params.pop("keyword")
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
            items, p = self.paginate(perm_datas, **p)
            self.resp([qe.to_dict(i) for i in perm_datas], **p)

    def patch(self):
        params = self.get_json_args(Schema({
            "cmdb_id": scm_gt0_int,
            "role_id": scm_gt0_int,
            "schema_names": [scm_unempty_str]
        }))
        role_id = params.pop("role_id")
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
            session.query(RoleDataPrivilege). \
                filter(
                RoleDataPrivilege.role_id == role_id,
                RoleDataPrivilege.cmdb_id == cmdb_id). \
                delete(synchronize_session='fetch')
            session.add_all([RoleDataPrivilege(
                cmdb_id=cmdb_id,
                role_id=role_id,
                schema_name=i
            ) for i in schema_names])

        clear_cache.delay()
        return self.resp_created(msg="分配权限成功")

    def delete(self):
        params = self.get_json_args(Schema({
            'cmdb_id': scm_int,
            'role_id': scm_int,
            'schema_name': scm_unempty_str
        }))
        with make_session() as session:
            session.query(RoleDataPrivilege).filter_by(**params).delete()

        clear_cache.delay()
        self.resp_created(msg="删除成功")
