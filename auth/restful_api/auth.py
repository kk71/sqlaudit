# Author: kk.Fang(fkfkbill@gmail.com)

import jwt

import settings
from models.sqlalchemy import *
from restful_api.modules import as_view
from restful_api.base import *
from utils.datetime_utils import *
from utils.schema_utils import *
from ..user import User
from .base import *


@as_view("login", group="auth")
class AuthHandler(BaseReq):

    def post(self):
        """登录"""

        params = self.get_json_args(Schema({
            "login_user": scm_unempty_str,
            "password": scm_unempty_str  # 此处需前端传来md5处理过的密码，而非明文
        }))

        with make_session() as session:
            if session.query(User).filter_by(
                    login_user=params["login_user"]).count() == 1:
                params["status"] = True
                user = session.query(User).filter_by(**params).first()
                if not user:
                    user = session.query(User).filter_by(
                        login_user=params["login_user"]).first()
                    user.last_login_failure_time = arrow.now().datetime
                    user.login_retry_counts += 1
                    session.add(user)
                    return self.resp_bad_username_password(
                        msg="请检查用户名密码，并确认该用户是启用状态。")
                # login successfully
                user.last_login_ip = self.request.remote_ip
                user.login_counts = user.login_counts + 1
                user.last_login_time = arrow.now().datetime
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
                return self.resp_created(content)
            self.resp_bad_username_password(
                msg="请检查用户名密码，并确认该用户是启用状态。")

    post.argument = {
        "querystring": {},
        "json": {}
    }


@as_view("current", group="auth")
class CurrentUserHandler(AuthReq):

    def get(self):
        """查看token的登录用户信息"""
        with make_session() as session:
            current_user_object = session.query(User). \
                filter(User.login_user == self.current_user).first()
            if not current_user_object:
                return self.resp_unauthorized(msg="当前登录用户不存在。")
            self.resp(current_user_object.to_dict())
