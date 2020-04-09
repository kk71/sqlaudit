# Author: kk.Fang(fkfkbill@gmail.com)

import json
from os import environ
from contextlib import contextmanager
from typing import *
from types import FunctionType

import cx_Oracle
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, String, Integer, Boolean, Sequence, Float
from sqlalchemy.dialects.oracle import DATE
from prettytable import PrettyTable

from utils.datetime_utils import *


ALL_ENV_VARS = list()


def env_get(k, default, parser=None):
    final_value = environ.get(k, default)
    if parser:
        default = parser(default)
        final_value = parser(final_value)
    ALL_ENV_VARS.append((k, default, final_value, "!" if default != final_value else ""))
    return final_value


# oracle connection settings

WEB_IP = env_get("WEB_IP", "192.0.0.9")
ORM_ECHO = env_get("ORM_ECHO", False, int)
ORACLE_IP = env_get("ORACLE_IP", WEB_IP)
ORACLE_USERNAME = env_get("ORACLE_USERNAME", "isqlaudit")
ORACLE_PASSWORD = env_get("ORACLE_PASSWORD", "v1g2m60id2499yz")
ORACLE_PORT = env_get("ORACLE_PORT", "1521")
ORACLE_SID = env_get("ORACLE_SID", "sqlaudit")
ORACLE_SERVICE_NAME = env_get("ORACLE_SERVICE_NAME", "sqlaudit")

pt = PrettyTable(["environment variable", "default", "final", "different"])
for r in ALL_ENV_VARS:
    pt.add_row(r)
print(pt)


# make connect to old oracle

oracle_dsn = cx_Oracle.makedsn(ORACLE_IP, ORACLE_PORT, sid=ORACLE_SID)
engine = create_engine(
    f"oracle://{ORACLE_USERNAME}:{ORACLE_PASSWORD}@{oracle_dsn}",
    echo=ORM_ECHO)
base = declarative_base()
Session = sessionmaker(bind=engine)


@contextmanager
def make_oracle_session():
    session = Session()
    try:
        yield session
        session.commit()
    except Exception as e:
        session.rollback()
        print(f"session object {id(session)} has"
              f" been rolled-back because of the following exception: ")
        raise e
    finally:
        session.close()


class QueryEntity(List):

    def __init__(self, *args, **kwargs):
        super(QueryEntity, self).__init__(args)
        self.keys = [i.key for i in self]

    def to_dict(self, v, datetime_to_str: bool = True):
        if datetime_to_str:
            v = [dt_to_str(i) if isinstance(i, datetime) else i for i in v]
        return dict(zip(self.keys, v))

    @classmethod
    def to_plain_list(cls, v):
        """如果只有单个查询参数，将其展开成为单个list"""
        return [i[0] for i in v]


class BaseModel(base):
    __abstract__ = True

    def from_dict(self,
                  d: dict,
                  iter_if: FunctionType = None,
                  iter_by: FunctionType = None,
                  ) -> NoReturn:
        """update a record by given dict,
        with an iter function(mostly a lambda) to judge whether applies the change"""
        for k, v in d.items():
            if k not in dir(self):
                continue
            if isinstance(iter_if, FunctionType) and not iter_if(k, v):
                continue
            if iter_by:
                v = iter_by(k, v)
            setattr(self, k, v)

    def to_dict(self,
                iter_if: FunctionType = None,
                iter_by: FunctionType = None,
                datetime_to_str: bool = True
                ) -> dict:
        d = {}
        for k in self.__dict__:
            if k in ("_sa_instance_state",):
                continue
            v = getattr(self, k)
            if isinstance(iter_if, FunctionType) and not iter_if(k, v):
                continue
            if iter_by:
                v = iter_by(k, v)
            d[k] = v
            if datetime_to_str and isinstance(d[k], datetime):
                d[k] = arrow.get(d[k]).format(const.COMMON_DATETIME_FORMAT)
            elif datetime_to_str and isinstance(d[k], date):
                d[k] = arrow.get(d[k]).format(const.COMMON_DATE_FORMAT)
        return d

    def __str__(self):
        return json.dumps(self.to_dict())


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

    id = Column("ID", Integer, Sequence("SEQ_ROLE_PRIVILEGE"), primary_key=True)
    role_id = Column("ROLE_ID", Integer)
    privilege_type = Column("PRIVILEGE_TYPE", Integer)
    privilege_id = Column("PRIVILEGE_ID", Integer)
    create_date = Column("CREATE_DATE", DATE, default=datetime.now)


class Role(BaseModel):
    __tablename__ = "T_ROLE"

    role_id = Column("ROLE_ID", Integer, Sequence("SEQ_ROLE"), primary_key=True)
    role_name = Column("ROLE_NAME", String)
    create_date = Column("CREATE_DATE", DATE, default=datetime.now)
    comments = Column("COMMENTS", String)


class CMDB(BaseModel):
    """客户纳管数据库"""
    __tablename__ = "T_CMDB"

    cmdb_id = Column("CMDB_ID", Integer, Sequence('SEQ_CMDB'), primary_key=True)
    connect_name = Column("CONNECT_NAME", String)
    group_name = Column("GROUP_NAME", String)
    business_name = Column("BUSINESS_NAME", String)
    machine_room = Column("MACHINE_ROOM", Integer)
    database_type = Column("DATABASE_TYPE", Integer)  # TODO 重构的时候把这个字段统一为str
    server_name = Column("SERVER_NAME", String)
    ip_address = Column("IP_ADDRESS", String)
    port = Column("PORT", Integer)
    service_name = Column("SERVICE_NAME", String)
    user_name = Column("USER_NAME", String)
    password = Column("PASSWORD", String)
    is_collect = Column("IS_COLLECT", Boolean)
    create_date = Column("CREATE_DATE", DATE, default=lambda: datetime.now().date())
    create_owner = Column("CREATE_OWNER", String)
    status = Column("STATUS", Boolean)
    auto_sql_optimized = Column("AUTO_SQL_OPTIMIZED", Boolean)
    domain_env = Column("DOMAIN_ENV", Integer)
    is_rac = Column("IS_RAC", Boolean)
    white_list_status = Column("WHITE_LIST_STATUS", Boolean)
    while_list_rule_counts = Column("WHITE_LIST_RULE_COUNTS", Integer)
    db_model = Column("DB_MODEL", String)
    baseline = Column("BASELINE", Integer)
    is_pdb = Column("IS_PDB", Boolean)
    version = Column("VERSION", String)
    sid = Column("SID", String)
    allow_online = Column("ALLOW_ONLINE", Boolean, default=False)


class RoleDataPrivilege(BaseModel):
    """角色数据库权限"""
    __tablename__ = "T_ROLE_DATA_PRIVILEGE"

    id = Column("ID", Integer, Sequence("SEQ_ROLE_DATA_PRIVILEGE_ID"), primary_key=True)
    role_id = Column("ROLE_ID", Integer)
    cmdb_id = Column("CMDB_ID", Integer)
    schema_name = Column("SCHEMA_NAME", String)
    comments = Column("COMMENTS", String)
    create_date = Column("CREATE_DATE", DATE, default=datetime.now)


class DataHealthUserConfig(BaseModel):
    """数据库评分配置"""
    __tablename__ = "T_DATA_HEALTH_USER_CONFIG"

    # database_name实际存放connect_name！
    database_name = Column("DATABASE_NAME", String, primary_key=True)
    username = Column("USERNAME", String)
    needcalc = Column("NEEDCALC", String, default=1)
    weight = Column("WEIGHT", Float, default=1)
