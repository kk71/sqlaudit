# Author: kk.Fang(fkfkbill@gmail.com)

from datetime import datetime

from sqlalchemy import Column, String, Integer, Boolean, Sequence
from sqlalchemy.dialects.oracle import DATE

from .utils import BaseModel


class User(BaseModel):
    __tablename__ = "T_USER"

    login_user = Column("LOGIN_USER", String, primary_key=True)
    user_name = Column("USER_NAME", String)
    password = Column("PASSWORD", String)
    email = Column("EMAIL", String)
    mobile_phone = Column("MOBILE_PHONE", String)
    department = Column("DEPARTMENT", String)
    status = Column("STATUS", Boolean, default=True)
    last_login_time = Column("LAST_LOGIN_TIME", DATE)
    last_login_ip = Column("LAST_LOGIN_IP", String)
    login_counts = Column("LOGIN_COUNTS", Integer, default=0)
    login_retry_counts = Column("LOGIN_RETRY_COUNTS", Integer, default=0)
    last_login_failure_time = Column("LAST_LOGIN_FAILURE_TIME", DATE)
    create_date = Column("CREATE_DATE", DATE, default=datetime.now)
    comments = Column("COMMENTS", String)
    top_cmdb_ids = Column("TOP_CMDB_IDS", Integer)
    col = Column("COL", DATE)


class UserRole(BaseModel):
    __tablename__ = "T_USER_ROLE"

    login_user = Column("LOGIN_USER", String, primary_key=True)
    role_id = Column("ROLE_ID", Integer, primary_key=True)
    create_date = Column("CREATE_DATE", DATE, default=datetime.now)


class RolePrivilege(BaseModel):
    __tablename__ = "T_ROLE_PRIVILEGE"

    role_id = Column("ROLE_ID", Integer, primary_key=True)
    privilege_type = Column("PRIVILEGE_TYPE", Integer)
    privilege_id = Column("PRIVILEGE_ID", Integer)
    create_date = Column("CREATE_DATE", DATE, default=datetime.now)


# class Privilege(BaseModel):
#     __tablename__ = "T_PRIVILEGE"
#
#     privilege_id = Column("PRIVILEGE_ID", Integer, primary_key=True)
#     privilege_name = Column("PRIVILEGE_NAME", String)
#     operate_url = Column("OPERATE_URL", String)
#     create_date = Column("CREATE_DATE", DATE, default=lambda: datetime.now())
#     comments = Column("COMMENTS", String)


class Role(BaseModel):
    __tablename__ = "T_ROLE"

    role_id = Column("ROLE_ID", Integer, Sequence("SEQ_ROLE"), primary_key=True)
    role_name = Column("ROLE_NAME", String)
    create_date = Column("CREATE_DATE", DATE, default=datetime.now)
    comments = Column("COMMENTS", String)
