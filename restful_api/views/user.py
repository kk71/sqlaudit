# Author: kk.Fang(fkfkbill@gmail.com)

import jwt
import arrow
from schema import Schema, Optional
from sqlalchemy.exc import IntegrityError

import settings
from utils.schema_utils import *
from .base import *
from utils import role_utils
from models.oracle import *


class AuthHandler(BaseReq):
    def post(self):
        """登录"""
        params = self.get_json_args(Schema({
            "login_user": scm_unempty_str,
            "password": scm_unempty_str
        }))
        with make_session() as session:
            if session.query(User).filter_by(login_user=params["login_user"]).count() == 1:
                params["status"] = 1
                user = session.query(User).filter_by(**params).first()
                if not user:
                    user = session.query(User).filter_by(login_user=params["login_user"]).first()
                    user.last_login_failure_time = datetime.now().date()
                    user.login_retry_counts += 1
                    session.add(user)
                    self.resp_unauthorized(msg="请检查密码，并确认该用户是启用状态。")
                    return
                # login successfully
                user.last_login_ip = self.request.remote_ip
                user.login_counts = user.login_counts + 1 if user.login_counts else 0
                user.last_login_time = datetime.now().date()
                session.add(user)
                token = jwt.encode(
                        {
                            "login_user": user.login_user,
                            "timestamp": arrow.now().timestamp
                        },
                        key=settings.JWT_SECRET,
                        algorithm=settings.JWT_ALGORITHM
                )
                content = user.to_dict()
                content["token"] = token.decode("ascii")
                self.resp_created(content)
                return
            self.resp_unauthorized(msg="用户名错误。")


class UserHandler(BaseReq):
    def get(self):
        """用户列表"""
        params = self.get_query_args(Schema({
            Optional("roles", default=None): scm_dot_split_str,
            Optional("page", default=1): scm_int,
            Optional("per_page", default=10): scm_int,
        }))
        roles = params.pop("roles")
        with make_session() as session:
            q = session.query(User, UserRole).\
                join(UserRole, User.login_user == UserRole.login_user).filter_by()
            if roles:
                # 仅返回指定角色id的用户，如果不给参数则意味着返回全部用户
                q = q.filter(UserRole.role_id.in_(roles))
            items, p = self.paginate(q, **params)
            ret = []
            for user, user_role in items:
                ret.append({
                    **user.to_dict(),
                    **user_role.to_dict(iter_if=lambda k, v: k in ("role_id",))
                })
            self.resp(ret, **p)

    def post(self):
        """新增用户"""
        params = self.get_json_args(Schema({
            "login_user": scm_unempty_str,
            "user_name": scm_unempty_str,
            "password": scm_unempty_str,
            "email": scm_unempty_str,
            "mobile_phone": scm_str,
            "department": scm_str,
            "status": scm_int,
            "role_id": scm_one_of_choices(role_utils.ALL_ROLES)
        }))
        role_id = params.pop("role_id")
        with make_session() as session:
            try:
                new_user = User(**params)
                new_role_user_rel = UserRole(login_user=params["login_user"], role_id=role_id)
                session.add_all([new_user, new_role_user_rel])
                session.commit()
            except IntegrityError:
                self.resp_forbidden(msg="用户名已存在，请修改后重试")
            session.refresh(new_user)
            session.refresh(new_role_user_rel)
            self.resp_created({**new_user.to_dict(), **new_role_user_rel.to_dict()})

    def patch(self):
        """修改用户"""
        params = self.get_json_args(Schema({
            "login_user": scm_unempty_str,
            Optional("user_name"): scm_unempty_str,
            Optional("password"): scm_unempty_str,
            Optional("email"): scm_unempty_str,
            Optional("mobile_phone"): scm_str,
            Optional("department"): scm_str,
            Optional("status"): scm_int,
            Optional("role_id"): scm_one_of_choices(role_utils.ALL_ROLES)
        }))
        with make_session() as session:
            the_user = session.query(User).filter_by(login_user=params.pop("login_user")).first()
            the_user.from_dict(params)
            the_user_role_rel = session.query(UserRole).filter_by(login_user=the_user.login_user).first()
            the_user_role_rel.from_dict(params, iter_if=lambda k, v: k in ("role_id",))
            session.add_all([the_user, the_user_role_rel])
            self.resp_created({**the_user.to_dict(), **the_user_role_rel.to_dict()})

    def delete(self):
        """删除用户"""
        params = self.get_json_args(Schema({
            "login_user": scm_unempty_str,
        }))
        with make_session() as session:
            the_user = session.query(User).filter_by(**params)
            session.delete(the_user)
        self.resp_created(msg="已删除。")


