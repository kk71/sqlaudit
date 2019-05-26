# Author: kk.Fang(fkfkbill@gmail.com)

from schema import Schema, Optional

from .base import *
from utils.schema_utils import *


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
        self.resp()

    def patch(self):
        self.resp_created()

    def delete(self):
        self.resp_created()


class CMDBRatingPermissionHandler(AuthReq):
    """数据库评分权限配置"""

    def get(self):
        self.resp()

    def patch(self):
        self.resp_created()

    def delete(self):
        self.resp_created()
