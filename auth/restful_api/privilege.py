# Author: kk.Fang(fkfkbill@gmail.com)

from .base import PrivilegeReq
from auth.user import *
from ..const import PRIVILEGE
from utils.schema_utils import *
from models.sqlalchemy import *
from restful_api.modules import *


@as_view(group="privilege")
class PrivilegeHandler(PrivilegeReq):

    def get(self):
        """权限列表"""
        params = self.get_query_args(Schema({
            scm_optional("type", default=PRIVILEGE.ALL_PRIVILEGE_TYPE): And(
                scm_dot_split_int,
                scm_subset_of_choices(PRIVILEGE.ALL_PRIVILEGE_TYPE)
            ),
            scm_optional("current_user", default=False): scm_bool,
            **self.gen_p(per_page=99)
        }))
        privilege_type: list = params.pop("type")
        current_user = params.pop("current_user")
        p = self.pop_p(params)

        if current_user:
            if self.is_admin():
                # admin用户拥有任何权限
                privilege_ids = PRIVILEGE.get_all_privilege_id()
            else:
                with make_session() as session:
                    privilege_ids = [
                        i[0]
                        for i in session.query(RolePrivilege.privilege_id).
                        join(UserRole, RolePrivilege.role_id == UserRole.role_id).
                        filter(UserRole.login_user == self.current_user)
                    ]
            privilege_dicts = [
                PRIVILEGE.privilege_to_dict(PRIVILEGE.get_privilege_by_id(i))
                for i in privilege_ids if PRIVILEGE.get_privilege_by_id(i)
            ]
            privilege_dicts = [
                i["name"]
                for i in privilege_dicts
                if i["type"] in privilege_type
            ]

        else:
            privilege_dicts = [
                PRIVILEGE.privilege_to_dict(i)
                for i in PRIVILEGE.get_privilege_by_type(privilege_type)
            ]

        items, p = self.paginate(privilege_dicts, **p)
        self.resp([i for i in items], **p)


