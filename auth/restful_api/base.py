# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "AuthReq",
    "PrivilegeReq"
]

from typing import NoReturn, Optional, Awaitable

import jwt
from schema import SchemaError

import settings
from restful_api.base import *
from utils.privilege_utils import *
from utils.schema_utils import *
from utils.datetime_utils import *
from utils.const import AdminRequired
from .. import const


class AuthReq(BaseReq):
    """a request handler with authenticating"""

    def __init__(self, *args, **kwargs):
        super(AuthReq, self).__init__(*args, **kwargs)

    def get_current_user(self) -> NoReturn:
        token = self.request.headers.get("token", None)
        try:
            if not token:
                raise Exception("No token is present.")
            payload = jwt.decode(token, key=settings.JWT_SECRET, algorithm=settings.JWT_ALGORITHM)
        except:
            self.current_user = None
            self.resp_unauthorized()
            return
        now_timestamp = arrow.now().timestamp
        try:
            data = Schema({
                "login_user": scm_unempty_str,
                "timestamp": object,
                scm_optional(object): object  # 兼容未来可能增加的字段
            }).validate(payload)
            # 验证token的超时
            if now_timestamp - data["timestamp"] > settings.JWT_EXPIRE_SEC:
                raise const.TokenExpiredException
            self.current_user: str = data["login_user"]
        except SchemaError:
            self.current_user = None
            self.resp_bad_req(msg="请求的token payload结构错误。")
            return
        except const.TokenExpiredException:
            self.current_user = None
            self.resp_unauthorized(msg="请重新登录。")
            return
        print(
            f'* {self.current_user} - {settings.JWT_EXPIRE_SEC - (now_timestamp - data["timestamp"])}s to expire - {token}')

    def prepare(self) -> Optional[Awaitable[None]]:
        self.get_current_user()

    def is_admin(self):
        return settings.ADMIN_LOGIN_USER == self.current_user

    def acquire_admin(self):
        """如果不是admin就报错"""
        if not self.is_admin():
            self.resp_forbidden(msg="仅限管理员操作。")
            raise AdminRequired


class PrivilegeReq(AuthReq):
    """a request handler with role & privilege check"""

    def __init__(self, *args, **kwargs):
        super(AuthReq, self).__init__(*args, **kwargs)

    def should_have(self, *args):
        """judge what privilege is not present for current user"""
        if self.is_admin():
            return set()  # 如果是admin用户，则认为任何权限都是拥有的
        privilege_list = get_privilege_towards_user(self.current_user)
        return set(args) - set(privilege_list)

    def has(self, *args):
        """judge if privilege is all present for current user"""
        if self.should_have(*args):
            return False
        return True

    def acquire(self, *args):
        """ask for privilege, if not, response forbidden"""
        unavailable_privileges = self.should_have(*args)
        if unavailable_privileges:
            unavailable_privileges_names = ", ".join([
                PRIVILEGE.privilege_to_dict(i)["name"] for i in unavailable_privileges])
            self.resp_forbidden(msg=f"权限不足：{unavailable_privileges_names}")
            raise PrivilegeRequired

    def current_roles(self) -> list:
        """returns role_ids to current user"""
        return list(get_role_of_user(self.current_user).get(self.current_user, set([])))
