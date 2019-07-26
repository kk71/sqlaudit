# Author: kk.Fang(fkfkbill@gmail.com)

import cx_Oracle

from models.oracle import *
from models.mongo import *
from utils.datetime_utils import *
from utils.perf_utils import *
from utils import score_utils

import plain_db.oracleob


def get_current_cmdb(session, user_login, id_name="cmdb_id") -> list:
    """
    获取某个用户可见的cmdb
    :param session:
    :param user_login:
    :param id_name: 返回的唯一键值，默认是cmdb_id，亦可选择connect_name
    :return: list of cmdb_id
    """
    if id_name == "cmdb_id":
        return [i[0] for i in session.query(DataPrivilege.cmdb_id.distinct()).
                filter(DataPrivilege.login_user == user_login)]
    elif id_name == "connect_name":
        return [i[0] for i in session.query(CMDB.connect_name.distinct()).
                filter(CMDB.cmdb_id == DataPrivilege.cmdb_id)]


def get_current_schema(session, user_login, cmdb_id) -> list:
    """
    获取某个用户可见的schema
    :param session:
    :param user_login:
    :param cmdb_id:
    :return: list of schema
    """
    q = session.query(DataPrivilege.schema_name).filter(
        DataPrivilege.login_user == user_login, DataPrivilege.cmdb_id == cmdb_id)
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
    # TODO make it cached!

    if user_login:
        all_connect_names: set = set(get_current_cmdb(session, user_login, id_name="connect_name"))
    else:
        all_connect_names: set = {i[0] for i in session.query(CMDB.connect_name)}
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


@timing(cache=r_cache)
def online_overview_using_cache(date_start, date_end, cmdb_id, schema_name):

    from utils import object_utils, rule_utils, sql_utils

    with make_session() as session:
        dt_now = arrow.get(date_start)
        dt_end = arrow.get(date_end)
        sql_num_active = []
        sql_num_at_risk = []

        # SQL count
        while dt_now < dt_end:
            sql_text_q = SQLText.objects(
                cmdb_id=cmdb_id,
                etl_date__gte=dt_now.datetime,
                etl_date__lt=dt_now.shift(days=+1).datetime,
            )
            if schema_name:
                sql_text_q = sql_text_q.filter(schema=schema_name)
            active_sql_num = len(sql_text_q.distinct("sql_id"))
            at_risk_sql_num = len(sql_utils.get_risk_sql_list(
                session=session,
                cmdb_id=cmdb_id,
                schema_name=schema_name,
                sql_id_only=True,
                date_range=(dt_now.date(), dt_now.shift(days=+1).date())
            ))
            sql_num_active.append({
                "date": dt_to_str(dt_now),
                "value": active_sql_num
            })
            sql_num_at_risk.append({
                "date": dt_to_str(dt_now),
                "value": at_risk_sql_num
            })
            dt_now = dt_now.shift(days=+1)

        # risk_rule_rank

        # 只需要拿到rule_name即可，不需要知道其他两个key,
        # 因为当前仅对某一个库做分析，数据库类型和db_model都是确定的
        risk_rule_name_sql_num_dict = {
            # rule_name: {...}
            r3key[2]: {
                "violation_num": 0,
                "schema_set": set(),
                **robj.to_dict(iter_if=lambda k, v: k in ("risk_name", "severity"))
            }
            for r3key, robj in rule_utils.get_risk_rules_dict(session).items()}
        results_q = Results.objects(
            cmdb_id=cmdb_id, create_date__gte=date_start, create_date__lte=date_end)
        if schema_name:
            results_q = results_q.filter(schema_name=schema_name)
        for result in results_q:
            for rule_name in risk_rule_name_sql_num_dict.keys():
                result_rule_dict = getattr(result, rule_name, None)
                if not result_rule_dict:
                    continue
                if result_rule_dict.get("records", []) or result_rule_dict.get("sqls", []):
                    risk_rule_name_sql_num_dict[rule_name]["violation_num"] += 1
                    risk_rule_name_sql_num_dict[rule_name]["schema_set"]. \
                        add(result.schema_name)
        risk_rule_rank = [
            {
                "rule_name": rule_name,
                "num": k["violation_num"],
                "risk_name": k["risk_name"],
                "severity": k["severity"],
            } for rule_name, k in risk_rule_name_sql_num_dict.items()
        ]

        risk_rule_rank = sorted(risk_rule_rank, key=lambda x: x["num"], reverse=True)

        # top 10 execution cost by sum and by average
        sqls = sql_utils.get_risk_sql_list(
            session=session,
            cmdb_id=cmdb_id,
            schema_name=schema_name,
            date_range=(date_start, date_end),
            sqltext_stats=False
        )
        sql_by_sum = [
            {"sql_id": sql["sql_id"], "time": sql["execution_time_cost_sum"]}
            for sql in sqls
        ]
        top_10_sql_by_sum = sorted(
            sql_by_sum,
            key=lambda x: x["time"],
            reverse=True
        )[:10]
        top_10_sql_by_sum.reverse()
        sql_by_average = [
            {"sql_id": sql["sql_id"], "time": sql["execution_time_cost_on_average"]}
            for sql in sqls
        ]
        top_10_sql_by_average = sorted(
            sql_by_average,
            key=lambda x: x["time"],
            reverse=True
        )[:10]
        top_10_sql_by_average.reverse()

        # physical size of current CMDB
        latest_task_record_id = score_utils.get_latest_task_record_id(session, cmdb_id)[cmdb_id]
        tablespace_sum= {}
        stats_phy_size_object = StatsCMDBPhySize.objects(
            task_record_id=latest_task_record_id, cmdb_id=cmdb_id).first()
        if stats_phy_size_object:
            tablespace_sum = stats_phy_size_object.to_dict(
                iter_if=lambda k, v: k in ("total", "used", "usage_ratio", "free"),
                iter_by=lambda k, v: round(v, 2) if k in ("usage_ratio",) else v)

        return {
            # 以下是按照给定的时间区间搜索的结果
            "sql_num": {"active": sql_num_active, "at_risk": sql_num_at_risk},
            "risk_rule_rank": risk_rule_rank,
            "sql_execution_cost_rank": {
                "by_sum": top_10_sql_by_sum,
                "by_average": top_10_sql_by_average
            },
            "risk_rates": rule_utils.get_risk_rate(
                session=session, cmdb_id=cmdb_id, date_range=(date_start, date_end)),
            # 以下是取最近一次扫描的结果
            "tablespace_sum": tablespace_sum,
        }


def __prefetch():
    arrow_now = arrow.now()
    date_end = arrow_now.shift(days=+1).date()
    date_start_week = arrow_now.shift(weeks=-1).date()
    date_start_month = arrow_now.shift(days=-30).date()
    with make_session() as session:
        for cmdb in session.query(CMDB.cmdb_id).all():
            cmdb_id = cmdb[0]
            online_overview_using_cache(
                date_start=date_start_week,
                date_end=date_end,
                cmdb_id=cmdb_id,
                schema_name=None
            )
            online_overview_using_cache(
                date_start=date_start_month,
                date_end=date_end,
                cmdb_id=cmdb_id,
                schema_name=None
            )


online_overview_using_cache.prefetch = __prefetch
del __prefetch
