# Author: kk.Fang(fkfkbill@gmail.com)

from datetime import datetime

from sqlalchemy import Column, String, Integer, Boolean, Sequence
from sqlalchemy.dialects.oracle import DATE

from .utils import BaseModel
from utils.const import *


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


class OverviewRate(BaseModel):
    """概览页的默认设置"""
    __tablename__ = "T_OVERVIEW_RATE"

    id = Column("ID", Integer, Sequence("SEQ_OVERVIEW_RATE"), primary_key=True)
    login_user = Column("LOGIN_USER", String)
    cmdb_id = Column("CMDB_ID", Integer)
    item = Column("ITEM", String)
    type = Column("TYPE", Integer)


class DataHealthUserConfig(BaseModel):
    """数据库评分配置"""
    __tablename__ = "T_DATA_HEALTH_USER_CONFIG"

    database_name = Column("DATABASE_NAME", String, primary_key=True)  # connect_name
    username = Column("USERNAME", String)
    needcalc = Column("NEEDCALC", String, default=RANKING_CONFIG_NEED_CALC)
    weight = Column("WEIGHT", Integer, default=1)


class Param(BaseModel):
    """存放机房、环境、各种杂乱信息的表，程序逻辑只读不写"""
    __tablename__ = "T_PARAM"

    param_id = Column("PARAM_ID", Integer, primary_key=True)
    param_value = Column("PARAM_VALUE", String)
    param_type = Column("PARAM_TYPE", Integer)


class Notice(BaseModel):
    """公告栏"""
    __tablename__ = "T_NOTICE"

    notice_id = Column("NOTICE_ID", Integer, default=1, primary_key=True)
    contents = Column("CONTENTS", String)
    update_date = Column("UPDATE_DATE", DATE, default=datetime.now, onupdate=datetime.now)
    update_user = Column("UPDATE_USER", String)
