# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleCMDB",
    "RoleCMDBSchema"
]

from sqlalchemy import Column, String, Boolean,Integer

from . import exceptions
from cmdb.cmdb import CMDB
from models.sqlalchemy import BaseModel
from .plain_db import OraclePlainConnector


class OracleCMDB(CMDB):
    """oracle纳管库"""

    is_rac = Column("is_rac", Boolean)
    is_pdb = Column("is_pdb", Boolean)
    sid = Column("sid", String)
    service_name = Column("service_name", String)

    def build_connector(self, **kwargs):
        ret_params = [
            "ip_address", "port", "username", "password"
        ]
        if self.sid:
            ret_params.append("sid")  # 优先考虑用sid去连接纳管库
        elif self.service_name:
            ret_params.append("service_name")
        else:
            raise exceptions.OracleCMDBBadConfigured(
                "neither sid nor service_name is set")
        return OraclePlainConnector(
            **self.to_dict(iter_if=lambda k, v: k in ret_params))


class RoleCMDBSchema(BaseModel):
    """角色-oracle库-schema的绑定关系"""
    __tablename__ = "role_cmdb_schema"

    id = Column("id", Integer)
    role_id = Column("role_id", Integer)
    cmdb_id = Column("cmdb_id", Integer)
    schema_name = Column("schema_name", String)
    comments = Column("comments", String)

