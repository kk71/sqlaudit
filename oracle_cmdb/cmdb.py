# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleCMDB"
]

from sqlalchemy import Column, String, Boolean

from cmdb.cmdb import CMDB
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
            assert 0
        return OraclePlainConnector(
            **self.to_dict(iter_if=lambda k, v: k in ret_params))
