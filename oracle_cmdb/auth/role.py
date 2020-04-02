# Author: kk.Fang(fkfkbill@gmail.com)

from sqlalchemy import Column, String, Integer

from models.sqlalchemy import BaseModel


class OracleRoleSchema(BaseModel):
    """角色绑定的纳管库schema"""
    __tablename__ = "oracle_role_schema"

    id = Column("id", Integer, primary_key=True)
    role_id = Column("role_id", Integer)
    cmdb_id = Column("cmdb_id", Integer)
    schema_name = Column("schema_name", String)
    comments = Column("comments", String)

