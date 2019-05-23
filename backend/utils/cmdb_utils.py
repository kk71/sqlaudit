# Author: kk.Fang(fkfkbill@gmail.com)

from typing import *

import cx_Oracle
from mongoengine import QuerySet as M_Query
from sqlalchemy.orm.query import Query as S_Query

import plain_db.oracleob
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


def get_latest_health_score_cmdb(session, user_login=None):
    """
    获取用户可见的cmdb最新的health score排名
    :param session:
    :param user_login: 当前登录用户名，如果不给，则表示返回全部数据库的排名
    :return: [{"connect_name": str, "health_score": int, "collect_date": datetime}, ...]
    """
    # TODO 这个函数可做缓存

    if user_login:
        all_connect_names: set = {i["connect_name"] for i in get_current_cmdb(session, user_login)}
    else:
        all_connect_names: set = {i[0] for i in session.query(CMDB).filter_by().with_entities(CMDB.connect_name)}
    dh_objects = session.query(DataHealth).filter_by().order_by(DataHealth.collect_date.desc()).all()
    ret = []
    for dh in dh_objects:
        dh_dict = dh.to_dict()
        if dh.database_name not in {i["connect_name"] for i in ret}:
            ret.append({
                "connect_name": dh_dict.pop("database_name"),
                **dh_dict
            })
        if len(ret) == len(all_connect_names):
            break
    # sort
    ret = sorted(ret, key=lambda x: x["health_score"], reverse=True)

    # 当健康数据小于期望总数，说明有些纳管数据库其实还没做分析，但是仍要把列表补全
    if len(ret) != len(all_connect_names):
        collected_connect_names: set = {i["connect_name"] for i in ret}
        for cn in all_connect_names:
            if cn not in collected_connect_names:
                ret.append({
                    "connect_name": cn,
                    "health_score": None,
                    "collect_date": None
                })
    return ret
