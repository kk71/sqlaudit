# Author: kk.Fang(fkfkbill@gmail.com)

from typing import Union

import cx_Oracle

from cmdb.cmdb import CMDB
from models.sqlalchemy import make_session
from models.sqlalchemy import *
from utils.datetime_utils import *
from utils.perf_utils import *
from utils import score_utils
from auth import privilege_utils
from plain_db.oracleob import OracleOB

import plain_db.oracleob


def get_cmdb_bound_schema(session, cmdb_or_cmdb_id: Union[object, int]):
    """
    获取某个纳管库当前绑定的全部schema列表
    :param session:
    :param cmdb_id:
    :return: 返回的schema列表即: RoleDataPrivilege里登记的绑定schema，以及DataHealthUserConfig的
    """
    if isinstance(cmdb_or_cmdb_id, CMDB):
        cmdb = cmdb_or_cmdb_id
    elif isinstance(cmdb_or_cmdb_id, int):
        cmdb = session.query(CMDB).filter(CMDB.cmdb_id == cmdb_or_cmdb_id).first()
    else:
        assert 0
    q_dhuc = session.\
        query(DataHealthUserConfig.username).\
        filter(DataHealthUserConfig.database_name == cmdb.connect_name)
    schemas = [i[0] for i in q_dhuc]
    q_rdp = session.\
        query(RoleDataPrivilege.schema_name).\
        filter(RoleDataPrivilege.cmdb_id == cmdb.cmdb_id)
    schemas.extend([i[0] for i in q_rdp])
    return list(set(schemas))


def check_cmdb_privilege(cmdb: Union[object, int]) -> tuple:
    """
    检查纳管库的访问权限
    :param cmdb: cmdb_id或者cmdb对象
    """
    # TODO 要求有的权限
    user_sys_privs = ("SELECT ANY TABLE",)
    cmdb_connector = OracleOB(
        host=cmdb.ip_address,
        port=cmdb.port,
        username=cmdb.user_name,
        password=cmdb.password,
        sid=cmdb.service_name,
        # service_name=cmdb.sid
    )
    sql = f"select * from user_sys_privs where username='{cmdb.user_name.upper()}'"
    ret = cmdb_connector.select_dict(sql, one=False)
    all_privileges = {i["privilege"] for i in ret}
    for priv in user_sys_privs:
        if priv not in all_privileges:
            print(f"* fatal: this privilege required: {priv} for {cmdb.user_name.upper()}")
            return False
    return True


def clean_unavailable_schema(session, cmdb_id: int = None):
    """
    从数据权限和数据库评分里面，删除纳管库中实际不存在的schema
    :param session:
    :param cmdb_id:
    :return:
    """
    q = session.query(CMDB)
    if cmdb_id:
        q = q.filter(CMDB.cmdb_id == cmdb_id)
    for cmdb in q:
        available_schemas: [str] = get_cmdb_available_schemas(cmdb)
        session.query(DataHealthUserConfig.username).filter(
            DataHealthUserConfig.database_name == cmdb.connect_name,
            DataHealthUserConfig.username.notin_(available_schemas)).\
            delete(synchronize_session=False)
        session.query(RoleDataPrivilege.schema_name).filter(
            RoleDataPrivilege.cmdb_id == cmdb.cmdb_id,
            RoleDataPrivilege.schema_name.notin_(available_schemas)).\
            delete(synchronize_session=False)
