# Author: kk.Fang(fkfkbill@gmail.com)

from datetime import datetime

from sqlalchemy import Column, String, Integer, Boolean, Sequence, Float
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




class DataHealthUserConfig(BaseModel):
    """数据库评分配置"""
    __tablename__ = "T_DATA_HEALTH_USER_CONFIG"

    # database_name实际存放connect_name！
    database_name = Column("DATABASE_NAME", String, primary_key=True)
    username = Column("USERNAME", String)
    # 注意： needcalc字段无实际意义！只要加入这张表的schema都一定会评分。
    needcalc = Column("NEEDCALC", String, default=RANKING_CONFIG_NEED_CALC)
    weight = Column("WEIGHT", Float, default=1)


