# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleCMDB"
]

from sqlalchemy import Column, String, Boolean,Integer

from cmdb.cmdb import CMDB


class OracleCMDB(CMDB):
    """oracle纳管库"""
    __tablename__ = "oracle_cmdb"

    id = Column("id", Integer, primary_key=True)
    is_rac = Column("is_rac", Boolean)
    is_pdb = Column("is_pdb", Boolean)
    service_name = Column("service_name", String)
    sid = Column("sid", String)

