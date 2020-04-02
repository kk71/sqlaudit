# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "CMDB"
]

from sqlalchemy import Column, String, Integer, Boolean, DateTime

from models.sqlalchemy import BaseModel
from utils.datetime_utils import *


class CMDB(BaseModel):
    """客户纳管数据库"""

    __tablename__ = "cmdb"

    cmdb_id = Column("cmdb_id", Integer, primary_key=True)
    connect_name = Column("connect_name", String)
    db_type = Column("db_type", Integer)
    group_name = Column("group_name", String)
    business_name = Column("business_name", String)
    server_name = Column("server_name", String)
    ip_address = Column("ip_address", String)
    port = Column("port", Integer)
    username = Column("username", String)
    password = Column("password", String)
    status = Column("status", Boolean)
    domain_env = Column("domain_env", Integer)
    db_model = Column("db_model", String)
    baseline = Column("baseline", Integer)
    version = Column("version", String)
    allow_online = Column("allow_online", Boolean, default=False)
    create_time = Column("create_time", DateTime, default=lambda: datetime.now().date())

    # for oracle only
    is_rac = Column("is_rac", Boolean)
    is_pdb = Column("is_pdb", Boolean)
    service_name = Column("service_name", String)
    sid = Column("sid", String)

