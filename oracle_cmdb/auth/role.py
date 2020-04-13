# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "RoleOracleCMDBSchema"
]

from sqlalchemy import Column, String, Integer

from models.sqlalchemy import BaseModel


class RoleOracleCMDBSchema(BaseModel):
    """角色-oracle纳管库-schema的绑定关系"""
    __tablename__ = "role_oracle_cmdb_schema"

    id = Column("id", Integer, primary_key=True)
    role_id = Column("role_id", Integer)
    cmdb_id = Column("cmdb_id", Integer)
    schema_name = Column("schema_name", String)
    comments = Column("comments", String)
