# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleCMDB",
    "RoleDataPrivilege"
]

from sqlalchemy import Column, String, Boolean,Integer

from cmdb.cmdb import CMDB
from models.sqlalchemy import BaseModel


class OracleCMDB(CMDB):
    """oracle纳管库"""
    id = Column("id", Integer, primary_key=True)
    is_rac = Column("is_rac", Boolean)
    is_pdb = Column("is_pdb", Boolean)
    service_name = Column("service_name", String)
    sid = Column("sid", String)

class RoleDataPrivilege(BaseModel):
    """角色的数据权限"""

    id = Column("id", Integer)
    role_id = Column("role_id", Integer)
    cmdb_id = Column("cmdb_id", Integer)
    schema_name = Column("schema_name", String)
    comments = Column("comments", String)

