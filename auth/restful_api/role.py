# Author: kk.Fang(fkfkbill@gmail.com)

from sqlalchemy.exc import IntegrityError

from auth.user import *
from .base import PrivilegeReq
from ..const import PRIVILEGE
from utils.schema_utils import *
from models.sqlalchemy import *
from restful_api.modules import as_view


@as_view(group="role")
class RoleHandler(PrivilegeReq):

    def get(self):
        """角色列表"""

        self.acquire(PRIVILEGE.PRIVILEGE_ROLE_MANAGE)

        params = self.get_query_args(Schema({
            scm_optional("keyword", default=None): scm_str,
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
                    "privileges": [
                        PRIVILEGE.privilege_to_dict(
                            PRIVILEGE.get_privilege_by_id(j.privilege_id)
                        )
                        for j in session.query(RolePrivilege).filter_by(
                            role_id=r["role_id"])
                        if PRIVILEGE.get_privilege_by_id(j.privilege_id)
                    ]
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
                self.resp_bad_req(msg="已经存在该角色")
                return
            role = Role(**params)
            session.add(role)
            session.commit()
            session.refresh(role)
            session.add_all([
                RolePrivilege(
                    role_id=role.role_id,
                    privilege_id=i["id"],
                    privilege_type=i["type"]
                ) for i in privileges
            ])
            self.resp_created({
                **role.to_dict(),
                "privileges": privileges
            })

    def patch(self):
        """编辑角色"""
        params = self.get_json_args(Schema({
            "role_id": scm_int,

            scm_optional("role_name"): scm_unempty_str,
            scm_optional("comments"): scm_str,
            scm_optional("privileges", default=None): [
                scm_one_of_choices(PRIVILEGE.get_all_privilege_id())
            ]
        }))
        role_id = params.pop("role_id")
        privileges = params.pop("privileges")

        if privileges:
            privileges = [
                PRIVILEGE.privilege_to_dict(PRIVILEGE.get_privilege_by_id(i))
                for i in privileges
                if PRIVILEGE.get_privilege_by_id(i)
            ]

        with make_session() as session:
            role = session.query(Role).filter_by(role_id=role_id).first()
            if not role:
                return self.resp_not_found(msg="role not found.")
            role.from_dict(params)
            session.add(role)
            session.commit()
            session.refresh(role)
            if privileges:
                session.query(RolePrivilege). \
                    filter(RolePrivilege.role_id == role_id). \
                    delete(synchronize_session=False)
                session.commit()
                session.add_all([RolePrivilege(
                    role_id=role.role_id,
                    privilege_id=i["id"],
                    privilege_type=i["type"]
                ) for i in privileges])
        self.resp_created(msg="finished.")


@as_view("role_user", group="role")
class RoleUserHandler(PrivilegeReq):

    def get(self):
        """获取用户角色信息"""

        self.acquire(PRIVILEGE.PRIVILEGE_ROLE_USER_MANAGE)

        params = self.get_query_args(Schema({
            scm_optional("role_id", default=None): scm_int,
            scm_optional("login_user", default=None): scm_unempty_str,
            scm_optional("keyword", default=None): scm_str,
            **self.gen_p()
        }))
        p = self.pop_p(params)
        keyword = params.pop("keyword")

        with make_session() as session:
            keys = QueryEntity(
                User.username,
                UserRole.role_id,
                Role.role_name,
                User.login_user
            )
            user_role = session.query(*keys). \
                join(Role, UserRole.role_id == Role.role_id). \
                join(User, UserRole.login_user == User.login_user)
            if keyword:
                user_role = self.query_keyword(user_role, keyword,
                                               User.username,
                                               Role.role_name)
            items, p = self.paginate(user_role, **p)
            self.resp([keys.to_dict(i) for i in items])

    def post(self):
        """用户绑定角色"""
        params = self.get_json_args(Schema({
            "role_id": scm_gt0_int,
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
            session.query(UserRole).filter_by(**params). \
                delete(synchronize_session=False)
        self.resp_created(msg="deleted")
