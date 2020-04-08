
import plain_db.oracleob


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

