# Author: kk.Fang(fkfkbill@gmail.com)

import jwt
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
                    self.resp_bad_username_password(msg="请检查用户名密码，并确认该用户是启用状态。")
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
            self.resp_bad_username_password(msg="请检查用户名密码，并确认该用户是启用状态。")


class UserHandler(AuthReq):
    def get(self):
        """用户列表"""
        params = self.get_query_args(Schema(self.gen_p()))
        p = self.pop_p(params)
        with make_session() as session:
            user_q = session.query(User)
            items, p = self.paginate(user_q, **p)
            self.resp([i.to_dict() for i in items], **p)

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
        }))
        with make_session() as session:
            try:
                new_user = User(**params)
                session.add(new_user)
                session.commit()
            except IntegrityError:
                self.resp_forbidden(msg="用户名已存在，请修改后重试")
            session.refresh(new_user)
            self.resp_created(new_user.to_dict())

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
        }))
        with make_session() as session:
            the_user = session.query(User).filter_by(login_user=params.pop("login_user")).first()
            the_user.from_dict(params)
            self.resp_created(the_user.to_dict())

    def delete(self):
        """删除用户"""
        params = self.get_json_args(Schema({
            "login_user": scm_unempty_str,
        }))
        with make_session() as session:
            the_user = session.query(User).filter_by(**params).first()
            session.delete(the_user)
        self.resp_created(msg="已删除。")
