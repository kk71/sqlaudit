# Author: kk.Fang(fkfkbill@gmail.com)

import cx_Oracle
from schema import Schema, Optional

from .base import AuthReq
from utils.schema_utils import *
from utils.datetime_utils import dt_to_str
from models.oracle import *
from utils.const import *


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
            self.resp([i.to_dict() for i in items], **p)

    def post(self):
        """增加角色"""
        params = self.get_json_args(Schema({
            "role_name": scm_unempty_str,
            "comments": scm_str,
            # "privilege_id": [int]
        }))
        with make_session() as session:
            if session.query(Role).filter_by(role_name=params["role_name"]).count():
                self.resp_forbidden(msg="已经存在该角色")
                return
            role = Role(**params)
            session.add(role)
            session.commit()
            session.refresh(role)
            # session.bulk_save_objects([RolePrivilege()])
            self.resp_created(role.to_dict())

    def patch(self):
        """编辑角色"""
        params = self.get_json_args(Schema({
            "role_id": scm_int,

            Optional("role_name"): scm_unempty_str,
            Optional("comments"): scm_str,
            # Optional("privilege_id"): [int],
            # Optional("privilege_id_append"): [int],
            # Optional("privilege_id_exclude"): [int]
        }))
        role_id = params.pop("role_id")
        with make_session() as session:
            role = session.query(Role).filter_by(role_id=role_id).first()
            role.from_dict(params)
            session.add(role)
            session.commit()
            session.refresh(role)
            self.resp_created(role.to_dict())

    def delete(self):
        """删除角色"""
        params = self.get_json_args(Schema({
            "role_id": scm_int,
        }))
        with make_session() as session:
            session.query(Role).filter_by(**params).delete()
            session.query(UserRole).filter_by(**params).delete()
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
                Role.role_name
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
        params = self.get_query_args(Schema(self.gen_p()))
        p = self.pop_p(params)
        items, p = self.paginate(PRIVILEGE.ALL_PRIVILEGE, **p)
        self.resp([PRIVILEGE.privilege_to_dict(i) for i in items], **p)


class CMDBPermissionHandler(AuthReq):
    """数据库权限配置"""

    def get(self):
        if self.current_user != "admin":
            return self.resp_bad_req("No Authority")

        with make_session() as session:
            perm_datas = session.query(DataPrivilege, CMDB, User). \
                join(CMDB, DataPrivilege.cmdb_id == CMDB.cmdb_id). \
                join(User, User.login_user == DataPrivilege.login_user). \
                with_entities(CMDB.connect_name, CMDB.cmdb_id,
                              DataPrivilege.schema_name, DataPrivilege.create_date,
                              DataPrivilege.comments, User.user_name, User.login_user)
            permisson_datas = []
            perm_datas = [list(x) for x in perm_datas]
            for perm_data in perm_datas:
                permisson_datas.append({'connect_name': perm_data[0], 'cmdb_id': perm_data[1],
                                        'schema_name': perm_data[2], 'create_date': dt_to_str(perm_data[3]),
                                        'comments': perm_data[4], 'user_name': perm_data[5],
                                        'login_user': perm_data[6]})

            user = session.query(User).filter(User.login_user != "admin"). \
                with_entities(User.login_user, User.user_name)
            # users=[user.to_dict() for user in users]
            users = []
            user = [list(x) for x in user]
            for u in user:
                users.append({'login_user': u[0], 'user_name': u[1]})

            cmdb = session.query(CMDB.cmdb_id, CMDB.connect_name).all()
            # cmdbs=[cmdb.to_dict() for cmdb in cmdbs]
            cmdbs = []
            cmdb = [list(x) for x in cmdb]
            for c in cmdb:
                cmdbs.append({'cmdb_id': c[0], 'connect_name': c[1]})
            self.resp({"permisson_datas": permisson_datas, "users": users, "cmdbs": cmdbs})

    def patch(self):
        params = self.get_json_args(Schema({
            "cmdb_id": scm_int,
            "login_user": scm_unempty_str,
            "schema_names": list
        }))
        login_user, cmdb_id, schema_names = \
            params.pop("login_user"), params.pop("cmdb_id"), params.pop("schema_names")
        del params
        with make_session() as session:
            existing_schemas = session.query(DataPrivilege).\
                filter(DataPrivilege.login_user == login_user, DataPrivilege.cmdb_id == cmdb_id).\
                with_entities(DataPrivilege.schema_name)
            existing_schemas = [list(existing_schema)[0] for existing_schema in existing_schemas]
            # loading = {f"{login_user}:{cmdb_id}:{schema}" for schema in schema_names}
            # existing = {f"{login_user}:{cmdb_id}:{schema}" for schema in existing_schemas}
            loading={(login_user,cmdb_id,schema) for schema in schema_names}
            existing={(login_user,cmdb_id,schema) for schema in existing_schemas}

            # insert_schemas = [x.split(":")[2] for x in loading - existing]
            # delete_schemas = [x.split(":")[2] for x in existing - loading]
            insert_schemas=[x[2] for x in loading-existing]
            delete_schemas=[x[2] for x in existing-loading]

            if insert_schemas:
                for schema in insert_schemas:
                    config = DataPrivilege()
                    config.login_user = login_user
                    config.cmdb_id = cmdb_id
                    config.schema_name = schema
                    session.add(config)
                return self.resp_created(msg="分配权限成功")
            if delete_schemas:
                session.query(DataPrivilege).filter(
                    DataPrivilege.login_user == login_user,
                    DataPrivilege.cmdb_id == cmdb_id,
                    DataPrivilege.schema_name.in_(delete_schemas)).\
                    delete(synchronize_session='fetch')
                return self.resp_created(msg="分配权限成功")

    def post(self):
        params = self.get_json_args(Schema({
            "cmdb_id": scm_int,
            "login_user": scm_unempty_str
        }))
        cmdb_id, login_user = params.pop("cmdb_id"), params.pop("login_user")
        del params

        from utils.cmdb_utils import get_cmdb_available_schemas
        with make_session() as session:
            cmdb_objects = session.query(CMDB).filter(CMDB.cmdb_id == cmdb_id)
            for cmdb_object in cmdb_objects:
                try:
                    schemas = get_cmdb_available_schemas(cmdb_object)
                except cx_Oracle.DatabaseError as err:
                    print(err)
                    return self.resp_bad_req(msg="无法连接到目标主机")
            authed_schemas = session.query(DataPrivilege).\
                filter(DataPrivilege.login_user == login_user,
                       DataPrivilege.cmdb_id == cmdb_id).\
                with_entities(DataPrivilege.schema_name)
            authed_schemas = [list(authed_schema)[0].upper() for authed_schema in authed_schemas]
            schemas = {schema: 1 if schema in authed_schemas else 0 for schema in schemas}
            self.resp_created({'schemas': schemas})

    def delete(self):
        params = self.get_json_args(Schema({
            'cmdb_id': scm_int,
            'login_user': scm_unempty_str,
            'schema_name': scm_unempty_str
        }))
        with make_session() as session:
            session.query(DataPrivilege).filter_by(**params).delete()
            self.resp_created(msg="删除权限成功")
