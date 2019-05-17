# Author: kk.Fang(fkfkbill@gmail.com)

from typing import *

from mongoengine import QuerySet as M_Query
from sqlalchemy.orm.query import Query as S_Query

from backend.models.oracle import *

# 数据库类型

DB_ORACLE = "oracle"
DB_MYSQL = "mysql"
ALL_SUPPORTED_DB_TYPE = (DB_ORACLE, DB_MYSQL)


def get_current_cmdb(session, user_login) -> list:
    """
    获取某个用户可见的cmdb
    :param session:
    :param user_login:
    :return: list of cmdb_id
    """
    q = session.query(DataPrivilege).filter(DataPrivilege.login_user==user_login)
    return list({i.cmdb_id for i in q})


def get_current_schema(session, user_login, cmdb_id) -> list:
    """
    获取某个用户可见的schema
    :param session:
    :param user_login:
    :param cmdb_id:
    :return: list of schema
    """
    q = session.query(DataPrivilege).filter(
        DataPrivilege.login_user==user_login, DataPrivilege.cmdb_id==cmdb_id)
    return list({i.schema_name for i in q})
