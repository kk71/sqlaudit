# Author: kk.Fang(fkfkbill@gmail.com)

from typing import Union

import cx_Oracle

from models.oracle import *
from utils.datetime_utils import *
from utils.perf_utils import *
from utils import privilege_utils
from plain_db.oracleob import OracleOB

import plain_db.oracleob


def get_current_cmdb(session, user_login, id_name="cmdb_id") -> [str]:
    """
    获取某个用户可见的cmdb
    :param session:
    :param user_login:
    :param id_name: 返回的唯一键值，默认是cmdb_id，亦可选择connect_name
    :return: [cmdb_id或者connect_name, ...]
    """
    role_ids: list = list(privilege_utils.get_role_of_user(login_user=user_login).
                        get(user_login, set([])))
    if id_name == "cmdb_id":
        return [i[0] for i in session.query(RoleDataPrivilege.cmdb_id.distinct()).
                join(CMDB, CMDB.cmdb_id == RoleDataPrivilege.cmdb_id).
                filter(RoleDataPrivilege.role_id.in_(role_ids))]
    elif id_name == "connect_name":
        return [i[0] for i in session.query(CMDB.connect_name.distinct()).
                join(RoleDataPrivilege, RoleDataPrivilege.cmdb_id == CMDB.cmdb_id).
                filter(RoleDataPrivilege.role_id.in_(role_ids))]


def get_current_schema(
        session,
        user_login: Union[str, list, tuple, None] = None,
        cmdb_id=None,
        verbose: bool = False,
        verbose_dict: bool = False,
        query_entity: Union[tuple, list] = (
            RoleDataPrivilege.cmdb_id,
            CMDB.connect_name,
            RoleDataPrivilege.role_id,
            RoleDataPrivilege.schema_name),
) -> list:
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


@timing()
def get_latest_health_score_cmdb(session, user_login=None, collect_month=6) -> list:
    """
    获取用户可见的cmdb最新的health score排名
    :param session:
    :param user_login: 当前登录用户名，如果不给，则表示返回全部数据库的排名
    :param collect_month: 仅搜索当前起前n个月的数据，没有搜索到的，当作无分数对待
    :return: [{"connect_name": str, "health_score": int, "collect_date": datetime}, ...]
    """
    if user_login:
        all_connect_names: set = set(get_current_cmdb(session, user_login, id_name="connect_name"))
    else:
        all_connect_names: set = {i[0] for i in session.query(CMDB.connect_name)}
    dh_objects = session.query(DataHealth).filter(
        DataHealth.collect_date >= arrow.now().shift(months=-collect_month).datetime,
        DataHealth.database_name.in_(all_connect_names)
    ).order_by(DataHealth.collect_date.desc()).all()
    ret = []
    for dh in dh_objects:
        dh_dict = dh.to_dict()
        if dh.database_name not in {i["connect_name"] for i in ret}:
            ret.append({
                "connect_name": dh_dict.pop("database_name"),
                **dh_dict
            })
        if all_connect_names.issubset({i["connect_name"] for i in ret}):
            break
    # sort
    ret = sorted(ret, key=lambda x: x["health_score"])

    # 当健康数据小于期望总数，说明有些纳管数据库其实还没做分析，但是仍要把列表补全
    collected_connect_names: set = {i["connect_name"] for i in ret}
    if not all_connect_names.issubset(collected_connect_names):
        for cn in all_connect_names:
            if cn not in collected_connect_names:
                ret.append({
                    "connect_name": cn,
                    "health_score": None,
                    "collect_date": None
                })
    return ret


def test_cmdb_connectivity(cmdb):
    """
    测试纳管数据库的连接性
    :param cmdb:
    :return:
    """
    conn = None
    try:
        dsn = cx_Oracle.makedsn(cmdb.ip_address, str(cmdb.port), cmdb.service_name)
        # TODO default timeout is too long(60s)
        conn = cx_Oracle.connect(cmdb.user_name, cmdb.password, dsn)
    except Exception as e:
        return {"connectivity": False, "info": str(e)}
    finally:
        conn and conn.close()
    return {"connectivity": True, "info": ""}


def get_cmdb_bound_schema(session, cmdb_or_cmdb_id: Union[CMDB, int]):
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


def check_cmdb_privilege(cmdb: Union[CMDB, int]) -> tuple:
    """
    检查纳管库的访问权限
    :param cmdb: cmdb_id或者cmdb对象
    """
    # TODO 要求有的权限
    user_sys_privs = ("SELECT ANY TABLE",)
    cmdb_connector = OracleOB(
        host=cmdb.cmdb_id,
        port=cmdb.port,
        username=cmdb.user_name,
        password=cmdb.password,
        sid=cmdb.service_name,
        service_name=cmdb.sid
    )
    sql = f"select * from user_sys_privs where username='{cmdb.user_name.upper()}'"
    ret = cmdb_connector.select_dict(sql)
    all_privileges = {i["privilege"] for i in ret}
    for priv in user_sys_privs:
        if priv not in all_privileges:
            print(f"* fatal: this privilege required: {priv} for {cmdb.user_name.upper()}")
            return False
    return True