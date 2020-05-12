# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "CMDB"
]

from sqlalchemy import Column, String, Integer, Boolean

from models.sqlalchemy import *
from .cmdb_task import CMDBTask


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

    def build_connector(self, **kwargs):
        """产生一个当前纳管库的连接器"""
        raise NotImplementedError

    @classmethod
    def build_connector_by_cmdb_id(cls, cmdb_id: int):
        """用cmdb_id创建纳管库的连接"""
        with make_session() as session:
            the_cmdb = session.query(cls).filter_by(cmdb_id=cmdb_id).first()
            return the_cmdb.build_connector()

    def cmdb_task(self, **kwargs) -> CMDBTask:
        """获取当前纳管库对象的纳管库任务对象"""
        session = self._sa_instance_state.session
        q = session.query(CMDBTask).filter(CMDBTask.cmdb_id == self.cmdb_id)
        if kwargs:
            q = q.filter_by(**kwargs)
        return q.first()

