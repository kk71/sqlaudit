# Author: kk.Fang(fkfkbill@gmail.com)

from collections import defaultdict

from sqlalchemy.exc import IntegrityError

import settings
from models.sqlalchemy import *
from .base import *
from ..user import *
from restful_api.modules import as_view
from utils.schema_utils import *
from ..const import PRIVILEGE


@as_view(group="auth")
class UserHandler(AuthReq):

    @classmethod
    def filter_user(cls, session, has_privilege: [str] = None) -> sqlalchemy_q:
        """过滤用户列表"""
        user_q = session.query(User)
        if has_privilege:
            login_users = [settings.ADMIN_LOGIN_USER]
            login_user_privilege_id_dict = defaultdict(set)
            qe = QueryEntity(User.login_user, RolePrivilege.privilege_id)
            login_user_privilege_id = session.query(*qe). \
                join(UserRole, User.login_user == UserRole.login_user). \
                join(RolePrivilege, UserRole.role_id == RolePrivilege.role_id). \
                filter(RolePrivilege.privilege_id.in_(has_privilege))
            for login_user, privilege_id in login_user_privilege_id:
                login_user_privilege_id_dict[login_user].add(privilege_id)
            for login_user, privilege_ids in login_user_privilege_id_dict.items():
                if privilege_ids == set(has_privilege):
                    login_users.append(login_user)
            user_q = user_q.filter(User.login_user.in_(login_users))
        return user_q

    def get(self):
        """用户列表"""
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
            self.resp(to_ret, **p)

    def post(self):
        """新增用户
        请注意password需要先md5编码再传入
        """
        params = self.get_json_args(Schema({
            "login_user": scm_unempty_str,
            "username": scm_unempty_str,
            "password": scm_unempty_str,
            "email": scm_str,
            "mobile_phone": scm_str,
            "department": scm_str,
            "status": scm_bool,
        }))
        with make_session() as session:
            try:
                new_user = User(**params)
                session.add(new_user)
                session.commit()
            except IntegrityError:
                self.resp_bad_req(msg="用户名已存在，请修改后重试")
            session.refresh(new_user)
            self.resp_created(new_user.to_dict())

    def patch(self):
        """修改用户
        请注意password需要先md5编码再传入
        """
        params = self.get_json_args(Schema({
            "login_user": scm_unempty_str,

            scm_optional("username"): scm_unempty_str,
            scm_optional("old_password", default=None): scm_unempty_str,
            scm_optional("password"): scm_unempty_str,
            scm_optional("email"): scm_unempty_str,
            scm_optional("mobile_phone"): scm_str,
            scm_optional("department"): scm_str,
            scm_optional("status"): scm_int,
        }))
        old_password = params.pop("old_password")

        with make_session() as session:
            the_user = session.query(User). \
                filter_by(login_user=params.pop("login_user")).first()
            if "password" in params.keys():
                if not self.is_admin() and the_user.password != old_password:
                    return self.resp_bad_req(msg="老密码不正确")

            the_user.from_dict(params)
            the_user = the_user.to_dict()
        self.resp_created(the_user)

    def delete(self):
        """删除用户"""
        params = self.get_json_args(Schema({
            "login_user": scm_unempty_str,
        }))
        with make_session() as session:
            the_user = session.query(User).filter_by(**params).first()
            session.delete(the_user)
        self.resp_created(msg="已删除。")

    post.argument = {
        "querystring": {},
        "json": {
            "login_user": "xyxy",
            "username": "dada",
            "password": "123scdlh",
            "email": "58864",
            "mobile_phone": "18883467900",
            "department": "xx",
            "status": "1",
        }
    }
    patch.argument = {
        "querystring": {},
        "json": {
            "login_user": "xyxy",
            "//username": "ee",
            "//old_password": "",
            "//password": "",
            "//email": "",
            "//mobile_phone": "",
            "//department": "",
            "//status": "",
        }
    }
    delete.argument = {
        "querystring": {},
        "json": {
            "login_user": "xyxy",
        }
    }
