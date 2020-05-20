# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleBaseReq",
    "OraclePrivilegeReq"
]

from typing import Union, List

from restful_api.base import *
from auth.restful_api.base import *
from models.sqlalchemy import *
from ..cmdb import *
from ..auth.user_utils import *


class OracleBaseReq(BaseReq):

    def cmdbs(self, session) -> sqlalchemy_q:
        return session.query(OracleCMDB)

    def cmdb_ids(self, session) -> List[int]:
        return QueryEntity.to_plain_list(
            self.cmdbs(session).with_entities(OracleCMDB.cmdb_id))


class OraclePrivilegeReq(OracleBaseReq, PrivilegeReq):
    """登录用户的oracle相关接口"""

    def cmdbs(self, session) -> sqlalchemy_q:
        q = super().cmdbs(session)
        if not self.is_admin():
            # 如果是admin用户则可见任何纳管库
            cmdb_ids = current_cmdb(session, login_user=self.current_user)
            q = q.filter(OracleCMDB.cmdb_id.in_(cmdb_ids))
        return q

    def schemas(
            self,
            session,
            the_cmdb: Union[int, OracleCMDB]) -> List[str]:
        """当前登录用户在某个库的绑定schema"""
        if isinstance(the_cmdb, OracleCMDB):
            the_cmdb = the_cmdb.cmdb_id
        elif isinstance(the_cmdb, int):
            pass
        else:
            assert 0
        return current_schema(
            session,
            login_user=self.current_user,
            cmdb_id=the_cmdb
        )

