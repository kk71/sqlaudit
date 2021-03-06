# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "User",
    "Role",
    "UserRole",
    "RolePrivilege"
]

from hashlib import md5

from sqlalchemy import Column, String, Integer, Boolean, DateTime

from models.sqlalchemy import BaseModel


class User(BaseModel):
    """用户"""
    __tablename__ = "user"

    login_user = Column("login_user", String, primary_key=True)
    username = Column("username", String)
    password = Column("password", String)
    email = Column("email", String)
    mobile_phone = Column("mobile_phone", String)
    department = Column("department", String)
    status = Column("status", Boolean, default=True)
    last_login_time = Column("last_login_time", DateTime)
    last_login_ip = Column("last_login_ip", String)
    login_counts = Column("login_counts", Integer, default=0)
    login_retry_counts = Column("login_retry_counts", Integer, default=0)
    last_login_failure_time = Column("last_login_failure_time", DateTime)
    comments = Column("comments", String)
    top_cmdb_ids = Column("top_cmdb_ids", Integer)

    def set_password(self, password: str):
        """更新md5密码"""
        if password:
            self.password = md5(password.encode("utf-8")).hexdigest()


class Role(BaseModel):
    """角色"""
    __tablename__ = "role"

    role_id = Column("role_id", Integer, primary_key=True, autoincrement=True)
    role_name = Column("role_name", String)
    comments = Column("comments", String)


class UserRole(BaseModel):
    """用户角色绑定关系"""
    __tablename__ = "user_role"

    id = Column("id", Integer, primary_key=True, autoincrement=True)
    login_user = Column("login_user", String, primary_key=True)
    role_id = Column("role_id", Integer, primary_key=True)


class RolePrivilege(BaseModel):
    """角色权限绑定关系"""
    __tablename__ = "role_privilege"

    id = Column("id", Integer, primary_key=True, autoincrement=True)
    role_id = Column("role_id", Integer)
    privilege_type = Column("privilege_type", Integer)
    privilege_id = Column("privilege_id", Integer)

