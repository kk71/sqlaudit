# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "get_privilege_towards_user",
    "PRIVILEGE",
    "PrivilegeRequired",
    "get_role_of_user"
]

from collections import defaultdict

from .const import *
from .exceptions import *
from models.sqlalchemy import *
from .user import *


def get_role_of_user(login_user: Union[str, list, tuple]) -> defaultdict:
    """
    获取某个用户，或者某一批用户所绑定的角色id
    :param login_user:
    :return: {login_user: {role_id, ...}, ...}
    """
    if isinstance(login_user, str):
        login_user = [login_user]
    ret = defaultdict(set)
    with make_session() as session:
        user_role_q = session.query(UserRole.login_user, UserRole.role_id).\
            filter(UserRole.login_user.in_(login_user))
        for u, r_id in user_role_q:
            ret[u].add(r_id)
    return ret


def get_privilege_towards_user(login_user):
    with make_session() as session:
        privilege_ids = [i[0] for i in session.query(RolePrivilege.privilege_id).
            join(UserRole, RolePrivilege.role_id == UserRole.role_id).
            filter(UserRole.login_user == login_user)]
    return [PRIVILEGE.get_privilege_by_id(i)
            for i in privilege_ids if PRIVILEGE.get_privilege_by_id(i)]
