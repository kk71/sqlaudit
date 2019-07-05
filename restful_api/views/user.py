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
from past.utils.product_license import *
from past.utils.log import webui_logger
logger = webui_logger("webui")


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
                login_user_privilege_id = session.query(*qe).\
                    join(UserRole, User.login_user == UserRole.login_user).\
                    join(RolePrivilege, UserRole.role_id == RolePrivilege.role_id).\
                    filter(RolePrivilege.privilege_id.in_(has_privilege))
                for login_user, privilege_id in login_user_privilege_id:
                    login_user_privilege_id_dict[login_user].add(privilege_id)
                for login_user, privilege_ids in login_user_privilege_id_dict.items():
                    if privilege_ids == set(has_privilege):
                        login_users.append(login_user)
                user_q = user_q.filter(User.login_user.in_(login_users))
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


class InterfaceHandler(BaseReq):
    STATUS_CODE_SUCCESS = 2000
    STATUS_CODE_REQUEST_ERROR = 4000
    STATUS_CODE_SERVER_ERROR = 5000
    """
       interface response standard：
       {
           "statusCode": 2000,
           "statusDesc": "describe the statusCode",
           "respBody": "the response data",
       }
       default response successfully without data
    """
    def __init__(self,*args,**kwargs):
        super(InterfaceHandler, self).__init__(*args, **kwargs)
        self.status_code_key = "statusCode"
        self.status_desc_key = "statusDesc"
        self.resp_body_key = "respBody"
        self.status_code = self.STATUS_CODE_SUCCESS
        self.status_desc = "success"
        self.resp_body = {}

    @staticmethod
    def check_type(val, val_default_type):
        if not isinstance(val, val_default_type):
            raise TypeError(f"val {val} type is {type(val)}not default type {val_default_type}")

    def check_type_status_code(self, status_code):
        self.check_type(status_code, int)

    def check_type_status_desc(self, status_desc):
        self.check_type(status_desc, str)

    def check_type_resp_body(self, resp_body):
        self.check_type(resp_body, dict)

    def resp(self, status_code=None, status_desc="", resp_body={}):
        status_code = status_code if status_code is not None else self.status_code
        status_desc = status_desc if status_desc else self.status_desc
        resp_body = resp_body if resp_body else self.resp_body
        self.check_type_resp_body(resp_body)
        self.check_type_status_code(status_code)
        self.check_type_status_desc(status_desc)
        data = {
            self.status_code_key: status_code,
            self.status_desc_key: status_desc,
            self.resp_body_key: resp_body,
        }
        logger.info(f"[resp] data {data}")
        return self.write(data)


class ServiceError(Exception):
    pass


class ProductLicenseHandler(InterfaceHandler):
    name = "ProductLicenseHandler>"
    CALLBACK_TIME = 24 * 60 * 60 * 1000

    def __init__(self, *args, **kwargs):
        super(ProductLicenseHandler, self).__init__(*args, **kwargs)

    def get(self):
        flag = self.name + ">get"
        DEFAULT_DISPLAY_LENGTH = 128
        license_key = ""
        try:
            license_key = SqlAuditLicenseKeyManager.latest_license_key()
            license_key_ins = SqlAuditLicenseKey.decode(license_key)
            if not license_key_ins.is_valid():
                raise DecryptError("license info is invalid")
            self.resp_body = {
                'enterprise_name': license_key_ins.enterprise_name,
                'database_counts': license_key_ins.database_counts,
                'license_status': license_key_ins.license_status,
                'license_code': license_key[:DEFAULT_DISPLAY_LENGTH]
                if len(license_key) > DEFAULT_DISPLAY_LENGTH else license_key,
            }
        except DecryptError as e:
            logger.info("license expired or invalid")
            self.status_code = self.STATUS_CODE_REQUEST_ERROR
            self.status_desc = DecryptError.__name__
        except Exception as e:
            logger.error(f'{flag} error:', exc_info=1)
            logger.warning(f'{flag} license_key {license_key} \n')
            self.status_code = self.STATUS_CODE_SERVER_ERROR
            self.status_desc = ServiceError.__name__
        return self.resp()

    def post(self, *args, **kwargs):
        """
        receive:
        {
            "license": xxx
        }
        resp:
        resp_body = {
            "detail":"the license code is invalid and save success"
        }
        :param args:
        :param kwargs:
        :return:
        """
        flag = self.name + ">post"
        receive_license = license_info = ""
        try:
            # 前端要通过form-data传值，否则接收不到(应该是数据太长了,长度限制)
            receive_license=self.get_argument("license",None)
            if not SqlAuditLicenseKey.decode(receive_license).is_valid():
                logger.info(f"{flag} receive_license {receive_license}")
                raise DecryptError(f"{flag} the license code is invalid")
            SqlAuditLicenseKeyManager(receive_license,
                                      SqlAuditLicenseKeyManager.DB_LICENSE_STATUS["valid"],
                                      "get from website").insert()
            self.resp_body["detail"] = "license code is valid and save successfully"

        except DecryptError as e:
            logger.error(f"{flag} error: ", exc_info=1)
            self.status_code = self.STATUS_CODE_REQUEST_ERROR
            self.status_desc = DecryptError.__name__
        except Exception as e:
            logger.warning(f"{flag} receive_license {receive_license}")
            logger.warning(f"{flag} license_info {license_info}")
            logger.error(f"{flag} error: ", exc_info=1)
            self.status_code = self.STATUS_CODE_SERVER_ERROR
            self.status_desc = ServiceError.__name__
        return self.resp()