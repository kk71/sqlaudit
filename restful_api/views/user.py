# Author: kk.Fang(fkfkbill@gmail.com)

from collections import defaultdict

import jwt
from schema import Schema, Optional, And
from sqlalchemy.exc import IntegrityError

import settings
from utils.schema_utils import *
from utils.datetime_utils import *
from utils.const import *
from .base import *
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
        params = self.get_query_args(Schema({
            Optional("has_privilege", default=None):
                And(scm_dot_split_int, scm_subset_of_choices(PRIVILEGE.get_all_privilege_id())),
            **self.gen_p()
        }))
        p = self.pop_p(params)
        has_privilege = params.pop("has_privilege")
        with make_session() as session:
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
            items, p = self.paginate(user_q, **p)
            iter_by = lambda k, v: "***" if k == "password" and not self.is_admin() else v
            to_ret = [i.to_dict(iter_by=iter_by) for i in items]
            # TODO 这里给to_ret每个用户加上绑定的角色列表（包含角色id和角色名），
            #  以及纳管库的信息列表（connect_name, cmdb_id）
            for x in to_ret:
                keys = QueryEntity(
                    UserRole.role_id,
                    Role.role_name,
                    User.login_user
                )
                user_role = session.query(*keys). \
                    join(Role, UserRole.role_id == Role.role_id). \
                    join(User, UserRole.login_user == User.login_user)

                user_role=[list(x) for x in user_role]
                user_role=[y for y in user_role if x['login_user'] in y]

                x['role_id']=[a[0] for a in user_role]
                x['role_name']=[b[1] for b in user_role]

                qe = QueryEntity(CMDB.connect_name,
                             CMDB.cmdb_id,
                             RoleDataPrivilege.schema_name,
                             RoleDataPrivilege.create_date,
                             RoleDataPrivilege.comments,
                             Role.role_name,
                             Role.role_id)
                role_cmdb = session.query(*qe). \
                    join(CMDB, RoleDataPrivilege.cmdb_id == CMDB.cmdb_id). \
                    join(Role, Role.role_id == RoleDataPrivilege.role_id)

                role_cmdb=[list(x) for x in role_cmdb]
                role_cmdb=[b for b in role_cmdb for c in x['role_id'] if c in b]

                x['connect_name']=list(set([c[0] for c in role_cmdb]))
                x['cmdb_id']=list(set([d[1] for d in role_cmdb]))
                
            self.resp(to_ret, **p)

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
            the_user.from_dict(params,
                               iter_if=lambda k, v:
                               False if k == "password" and v == "***" else True)
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
