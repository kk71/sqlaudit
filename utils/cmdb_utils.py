# Author: kk.Fang(fkfkbill@gmail.com)

import cx_Oracle
from profilehooks import timecall

from models.oracle import *
from utils.datetime_utils import *

import plain_db.oracleob

# 数据库类型

DB_ORACLE = "oracle"
DB_MYSQL = "mysql"
ALL_SUPPORTED_DB_TYPE = (DB_ORACLE, DB_MYSQL)

# 纳管数据库的任务类型
DB_TASK_CAPTURE = "采集及分析"
DB_TASK_TUNE = "SQL智能优化"
ALL_DB_TASKS = (DB_TASK_CAPTURE, DB_TASK_TUNE)


class CMDBNotFoundException(Exception):
    """CMDB未找到错误"""
    pass


def get_current_cmdb(session, user_login) -> list:
    """
    获取某个用户可见的cmdb
    :param session:
    :param user_login:
    :return: list of cmdb_id
    """
    q = session.query(DataPrivilege).filter(DataPrivilege.login_user == user_login)
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


@timecall
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


@timecall
def get_latest_health_score_cmdb(session, user_login=None, collect_month=6):
    """
    获取用户可见的cmdb最新的health score排名
    :param session:
    :param user_login: 当前登录用户名，如果不给，则表示返回全部数据库的排名
    :param collect_month: 仅搜索当前起前n个月的数据，没有搜索到的，当作无分数对待
    :return: [{"connect_name": str, "health_score": int, "collect_date": datetime}, ...]
    """
    # TODO make it cached!

    if user_login:
        all_connect_names: set = {i["connect_name"] for i in get_current_cmdb(session, user_login)}
    else:
        all_connect_names: set = {i[0] for i in session.query(CMDB).filter_by().
            with_entities(CMDB.connect_name)}
    dh_objects = session.query(DataHealth).filter(
        DataHealth.collect_date >= arrow.now().shift(months=-collect_month).datetime
    ).order_by(DataHealth.collect_date.desc()).all()
    ret = []
    for dh in dh_objects:
        dh_dict = dh.to_dict()
        if dh.database_name not in {i["connect_name"] for i in ret}:
            ret.append({
                "connect_name": dh_dict.pop("database_name"),
                **dh_dict
            })
        if {i["connect_name"] for i in ret}.issubset(all_connect_names):
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
