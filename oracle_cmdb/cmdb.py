# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleCMDB",
    "RoleOracleCMDBSchema"
]

from sqlalchemy import Column, String, Boolean, Integer

import cmdb.const
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

    __mapper_args__ = {
        'polymorphic_identity': cmdb.const.DB_ORACLE
    }

    def build_connector(self, **kwargs) -> OraclePlainConnector:
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

    def get_available_schemas(self) -> [str]:
        """获取可用的全部schema"""
        sql = """
            SELECT username
            FROM dba_users
            WHERE username  NOT IN (
             'SYS', 'OUTLN', 'SYSTEM', 'CTXSYS', 'DBSNMP','DIP','ORACLE_OCM','APPQOSSYS','WMSYS','EXFSYS','CTXSYS','ANONYMOUS',
             'LOGSTDBY_ADMINISTRATOR', 'ORDSYS','XDB','XS$NULL','SI_INFORMTN_SCHEMA','ORDDATA','OLAPSYS','MDDATA','SPATIAL_WFS_ADMIN_USR',
             'ORDPLUGINS', 'OEM_MONITOR', 'WKSYS', 'WKPROXY','SPATIAL_CSW_ADMIN_USR','SPATIAL_CSW_ADMIN_USR','SYSMAN','MGMT_VIEW','FLOWS_FILES',
             'WK_TEST', 'WKUSER', 'MDSYS', 'LBACSYS', 'DMSYS','APEX_030200','APEX_PUBLIC_USER','OWBSYS','OWBSYS_AUDIT','OSE$HTTP$ADMIN',
             'WMSYS', 'OLAPDBA', 'OLAPSVR', 'OLAP_USER','SCOTT','AURORA$JIS$UTILITY$','BLAKE','JONES','ADAMS','CLARK','MTSSYS',
             'OLAPSYS', 'EXFSYS', 'SYSMAN', 'MDDATA','AURORA$ORB$UNAUTHENTICATED', 'SI_INFORMTN_SCHEMA', 'XDB', 'ODM')
            ORDER BY username ASC
            """
        schemas = [x[0] for x in self.build_connector().select(sql, one=False)]
        # TODO 需要判断 cx_Oracle.DatabaseError
        return schemas

    def test_connectivity(self) -> bool:
        """测试连接性"""
        try:
            self.build_connector()
        except Exception as e:
            return False
        return True


class RoleOracleCMDBSchema(BaseModel):
    """角色-oracle纳管库-schema的绑定关系"""
    __tablename__ = "role_oracle_cmdb_schema"

    id = Column("id", Integer, primary_key=True)
    role_id = Column("role_id", Integer)
    cmdb_id = Column("cmdb_id", Integer)
    schema_name = Column("schema_name", String)
    comments = Column("comments", String)
