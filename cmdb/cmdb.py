# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "CMDB"
]

from sqlalchemy import Column, String, Integer, Boolean

from models.sqlalchemy import *


class CMDB(BaseModel):
    """纳管数据库"""

    __tablename__ = "cmdb"

    cmdb_id = Column(
        "cmdb_id", Integer, primary_key=True, autoincrement=True)
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

    __mapper_args__ = {
        'polymorphic_on': db_type
    }

    def __str__(self):
        return f"CMDB-{self.db_type}-{self.cmdb_id}-{self.connect_name}"

    def build_connector(self, **kwargs):
        """产生一个当前纳管库的连接器"""
        raise NotImplementedError

    @classmethod
    def build_connector_by_cmdb_id(cls, cmdb_id: int):
        """用cmdb_id创建纳管库的连接"""
        with make_session() as session:
            the_cmdb = session.query(cls).filter_by(cmdb_id=cmdb_id).first()
            return the_cmdb.build_connector()

    def check_privilege(self):
        """检查纳管库是否具有必要的权限"""
        raise NotImplementedError

    @classmethod
    def drop(cls, cmdb_id: int):
        """删除纳管库"""
        with make_session() as session:
            from .cmdb_task import CMDBTask, CMDBTaskRecord
            from ticket.ticket import Ticket
            from ticket.sub_ticket import SubTicket
            session.query(CMDBTask).filter_by(cmdb_id=cmdb_id).delete(
                synchronize_session=False)
            session.query(CMDBTaskRecord).filter_by(cmdb_id=cmdb_id).delete(
                synchronize_session=False)
            Ticket.drop_cmdb_related_data(cmdb_id)
            SubTicket.drop_cmdb_related_data(cmdb_id)
            the_cmdb = session.query(CMDB).filter_by(cmdb_id=cmdb_id).first()
            session.delete(the_cmdb)
