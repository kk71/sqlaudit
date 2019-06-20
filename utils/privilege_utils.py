# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "get_privilege_towards_user",
    "PRIVILEGE",
    "PrivilegeRequired"
]

from utils.const import *
from models.oracle import *

PRIVILEGE_DICT = dict()
PRIVILEGE_DICT_TO_EXPIRE = 0
PRIVILEGE_DICT_TO_EXPIRE_MAX = 99


def get_privilege_towards_user(login_user):
    global PRIVILEGE_DICT, PRIVILEGE_DICT_TO_EXPIRE
    if PRIVILEGE_DICT_TO_EXPIRE > PRIVILEGE_DICT_TO_EXPIRE_MAX:
        PRIVILEGE_DICT = dict()
        PRIVILEGE_DICT_TO_EXPIRE = 0
    try:
        r = PRIVILEGE_DICT[login_user]
        return r
    except KeyError:
        with make_session() as session:
            privilege_ids = [i[0] for i in session.query(RolePrivilege.privilege_id).
                join(UserRole, RolePrivilege.role_id == UserRole.role_id).
                filter(UserRole.login_user == login_user)]
        PRIVILEGE_DICT[login_user] = [PRIVILEGE.get_privilege_by_id(i) for i in privilege_ids]
        return PRIVILEGE_DICT[login_user]
