
import cx_Oracle


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