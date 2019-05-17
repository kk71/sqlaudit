# Author: kk.Fang(fkfkbill@gmail.com)

from datetime import datetime

from sqlalchemy import Column, String, Integer, Boolean, Sequence
from sqlalchemy.dialects.oracle import DATE

from backend.models.oracle.utils import BaseModel


class CMDB(BaseModel):
    """客户纳管数据库"""
    __tablename__ = "T_CMDB"

    cmdb_id = Column("CMDB_ID", Integer, Sequence('SEQ_CMDB'), primary_key=True)
    connect_name = Column("CONNECT_NAME", String)
    group_name = Column("GROUP_NAME", String)
    business_name = Column("BUSINESS_NAME", String)
    machine_room = Column("MACHINE_ROOM", Integer)
    database_type = Column("DATABASE_TYPE", Integer)
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


class DataPrivilege(BaseModel):
    """数据库权限"""
    __tablename__ = "T_DATA_PRIVILEGE"

    login_user = Column("LOGIN_USER", String)
    cmdb_id = Column("CMDB_ID", Integer)
    schema_name = Column("SCHEMA_NAME", String)
    create_date = Column("CREATE_DATE", DATE, primary_key=True)
    comments = Column("COMMENTS", String)
