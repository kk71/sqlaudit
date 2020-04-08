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


def get_current_schema(
        session,
        user_login: Union[str, list, tuple, None] = None,
        cmdb_id=None,
        verbose: bool = False,
        verbose_dict: bool = False,
        query_entity: Union[tuple, list] = ()) -> list:
    """
    获取某个用户可见的schema
    :param session:
    :param user_login: 注意，如果传过来是多个login_user,并且返回不是verbose==True，并且还没传cmdb_id，
                        那么意味着多个库有重复的schema会被合并
    :param cmdb_id: 为None则表示拿全部绑定的schema
    :param verbose:
    :param verbose_dict:
    :param query_entity: 需要查询的字段
    :return: verbose==False:[schema_name, ...]
             verbose==True: [(*query_entity), ...]
             verbose_dict==True: [{*query_entity: values}, ...]
             所有返回结果都是list，并且去重。
    """
    qe = QueryEntity(*query_entity)
    if verbose or verbose_dict:
        q = session.query(*qe)
        models_to_join = set()
        for a_qe in qe:
            if a_qe.class_ in (CMDB, Role):
                models_to_join.add(a_qe.class_)
            elif a_qe.class_ == RoleDataPrivilege:
                pass
            else:
                assert 0
        for m in models_to_join:
            if m == CMDB:
                q = q.join(CMDB, CMDB.cmdb_id == RoleDataPrivilege.cmdb_id)
            if m == Role:
                q = q.join(Role, Role.role_id == RoleDataPrivilege.role_id)
    else:
        q = session.query(RoleDataPrivilege.schema_name.distinct())
    if user_login:
        role_ids: list = list(privilege_utils.get_role_of_user(login_user=user_login).
                              get(user_login, set([])))
        q = q.filter(RoleDataPrivilege.role_id.in_(role_ids))
    if cmdb_id:
        q = q.filter(RoleDataPrivilege.cmdb_id == cmdb_id)
    if verbose:
        return list(set(q))
    elif verbose_dict:
        return [qe.to_dict(i) for i in set(q)]
    else:
        return [i[0] for i in q]


@timing()
def get_cmdb_available_schemas(cmdb_object) -> [str]:
    """
    获取一个cmdb可用的全部schema
    :param cmdb_object:
    :return:
    """
    odb = plain_db.oracleob.OracleOB(
        cmdb_object.ip_address,
        cmdb_object.port,
        cmdb_object.user_name,
        cmdb_object.password,
        cmdb_object.service_name
    )
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
    schemas = [x[0] for x in odb.select(sql, one=False)]
    # TODO 需要判断 cx_Oracle.DatabaseError
    return schemas


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
