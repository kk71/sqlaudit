# Author: kk.Fang(fkfkbill@gmail.com)

import cx_Oracle
from schema import Schema, Optional

from .base import AuthReq
from utils.schema_utils import *
from utils.datetime_utils import dt_to_str
from models.oracle import *


class SystemPermissionHandler(AuthReq):
    """角色系统权限配置"""

    def get(self):
        """系统角色权限列表"""
        params = self.get_query_args(Schema({
            "role_id": scm_int,
            Optional("page", default=1): scm_int,
            Optional("per_page", default=50): scm_int,
        }))
        role_id = params.pop("role_id")

        # with make_session() as session:
        #     q = session.query(Privilege).all()
        #     itesm, p = self.paginate(q, **params)
        #     ret = {
        #         privilege_id: privilege
            # }
            #
            #
            #
            # q = session.query(Privilege).\
            #     join(RolePrivilege, Privilege.privilege_id == RolePrivilege.privilege_id).\
            #     filter(RolePrivilege.role_id == role_id)
            # items, p = self.paginate(q, **params)
            # ret = []
            # for a_privege, role_privilege in items:
            #     privilege_content = a_privege.to_dict()
            #     privilege_content.update({"enabled": False})
            #     if len(role_privilege):
            #         privilege_content.update({"enabled": True})
            #     ret.append(privilege_content)
            # self.resp(ret, **p)

            # privileges = session.query(RolePrivilege).\
            #     with_entities(RolePrivilege.role_id, RolePrivilege.privilege_id)
            # privileges = [list(x) for x in privileges]
            # res = defaultdict(list)
            # for priv in privileges:
            #     res[priv[0]].append(priv[1])
            #
            # roles = session.query(Role).with_entities(Role.role_id, Role.role_name)
            # roles = dict([list(x) for x in roles])
            #
            # user_privileges = {roles[x]: res[x] for x in roles}
            # sys_privileges = session.query(Privilege).\
            #     with_entities(Privilege.privilege_id, Privilege.comments)
            # sys_privileges = dict([list(x) for x in sys_privileges])
            # self.resp({'user_privileges': user_privileges, 'sys_privileges': sys_privileges})

    def patch(self):
        """部分更新系统权限"""
        params = self.get_json_args(Schema({
            "role_id": scm_int,
            Optional("privilege_ids"): scm_str,
        }))

        with make_session() as session:

            change_priv_ids = {int(x) for x in params["privilege_ids"].split(',')}
            priv_ids = session.query(RolePrivilege) \
                .filter(RolePrivilege.role_id == params["role_id"]).\
                with_entities(RolePrivilege.privilege_id)
            priv_ids = {priv_id[0] for priv_id in priv_ids}
            add_ids = change_priv_ids - priv_ids
            delete_ids = priv_ids - change_priv_ids

            for priv_id in add_ids:
                add_id = RolePrivilege(role_id=params["role_id"], privilege_id=priv_id)
                session.add(add_id)
            if delete_ids:  # TODO
                delete_id = session.query(RolePrivilege).filter(
                    RolePrivilege.role_id == params["role_id"],
                    RolePrivilege.privilege_id.in_([x for x in delete_ids])).all()
                session.delete(*delete_id)
        self.resp_created(msg="修改系统权限配置成功")


class CMDBPermissionHandler(AuthReq):
    """数据库权限配置"""

    def get(self):
        if self.current_user != "admin":
            return self.resp_bad_req("No Authority")

        with make_session() as session:
            perm_datas=session.query(DataPrivilege,CMDB,User).\
                join(CMDB,DataPrivilege.cmdb_id==CMDB.cmdb_id).\
                join(User,User.login_user==DataPrivilege.login_user).\
                with_entities(CMDB.connect_name,CMDB.cmdb_id,
                              DataPrivilege.schema_name,DataPrivilege.create_date,
                              DataPrivilege.comments,User.user_name,User.login_user)
            permisson_datas = []
            perm_datas = [list(x) for x in perm_datas]
            for perm_data in perm_datas:
                permisson_datas.append({'connect_name': perm_data[0], 'cmdb_id': perm_data[1],
                                    'schema_name': perm_data[2], 'create_date': dt_to_str(perm_data[3]),
                                    'comments': perm_data[4], 'user_name': perm_data[5],
                                    'login_user': perm_data[6]})

            user=session.query(User).filter(User.login_user != "admin").\
                with_entities(User.login_user,User.user_name)
            # users=[user.to_dict() for user in users]
            users = []
            user=[list(x) for x in user]
            for u in user:
                users.append({'login_user':u[0],'user_name':u[1]})

            cmdb=session.query(CMDB).with_entities(CMDB.cmdb_id,CMDB.connect_name)
            # cmdbs=[cmdb.to_dict() for cmdb in cmdbs]
            cmdbs = []
            cmdb =[list(x) for x in cmdb]
            for c in cmdb:
                cmdbs.append({'cmdb_id':c[0],'connect_name':c[1]})
            self.resp({"permisson_datas": permisson_datas, "users": users, "cmdbs": cmdbs})

    def patch(self):
        params=self.get_json_args(Schema({
            "cmdb_id":scm_int,
            "login_user":scm_unempty_str,
            "schema_names":list
        }))
        login_user,cmdb_id,schema_names=params.pop("login_user"),params.pop("cmdb_id"),params.pop("schema_names")
        del params
        with make_session() as session:
            existing_schemas=session.query(DataPrivilege).filter(DataPrivilege.login_user==login_user,
                DataPrivilege.cmdb_id==cmdb_id).\
                with_entities(DataPrivilege.schema_name)
            existing_schemas=[list(existing_schema)[0] for existing_schema in existing_schemas]
            loading={f"{login_user}:{cmdb_id}:{schema}"for schema in schema_names}
            existing={f"{login_user}:{cmdb_id}:{schema}" for schema in existing_schemas}

            insert_schemas=[x.split(":")[2] for x in (loading-existing)]
            delete_schemas=[x.split(":")[2] for x in (existing-loading)]

            if insert_schemas:
                for schema in insert_schemas:
                    config=DataPrivilege()
                    config.login_user=login_user
                    config.cmdb_id=cmdb_id
                    config.schema_name=schema
                    config.create_date=datetime.now()
                    session.add(config)
            if delete_schemas:
                session.query(DataPrivilege).filter(DataPrivilege.login_user==login_user,
                                                    DataPrivilege.cmdb_id==cmdb_id,
                                                    DataPrivilege.schema_name.in_(delete_schemas)).delete(synchronize_session='fetch')

            self.resp_created(msg="分配权限成功")

    def post(self):
        params=self.get_json_args(Schema({
            "cmdb_id":scm_int,
            "login_user":scm_unempty_str
        }))
        cmdb_id,login_user=params.pop("cmdb_id"),params.pop("login_user")
        del params

        from utils.cmdb_utils import get_cmdb_available_schemas
        with make_session() as session:
            cmdb_objects=session.query(CMDB).filter(CMDB.cmdb_id==cmdb_id)
            for cmdb_object in cmdb_objects:
                try:
                    schemas=get_cmdb_available_schemas(cmdb_object)
                except cx_Oracle.DatabaseError as err:
                    print(err)
                    return self.resp_bad_req(msg="无法连接到目标主机")
            authed_schemas=session.query(DataPrivilege).filter(DataPrivilege.login_user==login_user,DataPrivilege.cmdb_id==cmdb_id).\
                with_entities(DataPrivilege.schema_name)
            authed_schemas=[list(authed_schema)[0].upper() for authed_schema in authed_schemas]
            schemas={schema:1 if schema in authed_schemas else 0 for schema in schemas}
            self.resp_created({'schemas':schemas})

    def delete(self):
        params=self.get_json_args(Schema({
            'cmdb_id':scm_int,
            'login_user':scm_unempty_str,
            'schema_name':scm_unempty_str
        }))
        with make_session() as session:
            session.query(DataPrivilege).filter_by(**params).delete()

            self.resp_created("删除权限成功")

