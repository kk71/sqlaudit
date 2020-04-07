# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "CMDB"
]

from sqlalchemy import Column, String, Integer, Boolean

from models.sqlalchemy import BaseModel


class CMDB(BaseModel):
    """纳管数据库"""

    __tablename__ = "cmdb"

    cmdb_id = Column("cmdb_id", Integer, primary_key=True)
    connect_name = Column("connect_name", String)
    db_type = Column("db_type", String)
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

    def build_connector(self, **kwargs):
        """产生一个连接器"""
        raise NotImplementedError
