# -*- coding: utf-8 -*-"

from past.utils.oracle_connect import oracle_connect


def f_priv_db_user_list(**kwargs):
    l_dbinfo = kwargs["v_dbinfo"]
    conn = oracle_connect(
        l_dbinfo[0],
        l_dbinfo[1],
        l_dbinfo[3],
        l_dbinfo[4],
        l_dbinfo[2]
    )
    cursor = conn.cursor()
    cursor.execute(
    """select distinct owner from (
        SELECT /*+ parallel( a 4) */ OWNER,count(*) count1
        FROM dba_tables  a
        WHERE owner  NOT IN
        (
         'SYS', 'OUTLN', 'SYSTEM', 'CTXSYS', 'DBSNMP',
         'LOGSTDBY_ADMINISTRATOR', 'ORDSYS',
         'ORDPLUGINS', 'OEM_MONITOR', 'WKSYS', 'WKPROXY',
         'WK_TEST', 'WKUSER', 'MDSYS', 'LBACSYS', 'DMSYS',
         'WMSYS', 'OLAPDBA', 'OLAPSVR', 'OLAP_USER',
         'OLAPSYS', 'EXFSYS', 'SYSMAN', 'MDDATA',
         'SI_INFORMTN_SCHEMA', 'XDB', 'ODM','DBA','FLOWS_030000','DBMON','TIVOLI','FLOWS_FILES','OWBSYS',
          'APEX_050100','SQLAUTID','ANONYMOUS','SCOTT',
         'SQLAUD','ADMIN','APPQOSSYS','ORDDATA','ANONYMOUS','SQLAUDIT')   group by owner having count(*) >=1
        union
        select /*+ parallel(4) */ parsing_schema_name owner,count(*) count1 from dba_hist_sqlstat where
        parsing_schema_name NOT IN
        (
         'SYS', 'OUTLN', 'SYSTEM', 'CTXSYS', 'DBSNMP',
         'LOGSTDBY_ADMINISTRATOR', 'ORDSYS',
         'ORDPLUGINS', 'OEM_MONITOR', 'WKSYS', 'WKPROXY',
         'WK_TEST', 'WKUSER', 'MDSYS', 'LBACSYS', 'DMSYS',
         'WMSYS', 'OLAPDBA', 'OLAPSVR', 'OLAP_USER',
         'OLAPSYS', 'EXFSYS', 'SYSMAN', 'MDDATA',
         'SI_INFORMTN_SCHEMA', 'XDB', 'ODM','DBA','FLOWS_030000','DBMON','TIVOLI','APEX_050100','SQLAUTID','ANONYMOUS','SCOTT',
         'SQLAUD','ADMIN','APPQOSSYS','ORDDATA','ANONYMOUS','SQLAUDIT','FLOWS_FILES','OWBSYS'
         )
         group by parsing_schema_name having count(*) >=1   )  aa order by 1 asc
  """
    )
    records = cursor.fetchall()
    cursor.close()
    conn.close()
    l_return_stru = records
    return l_return_stru


if __name__ == "__main__":
    v_dbinfo = ["127.0.0.1", "1521", "ora11g1", "sqlaudit", "sqlaudit"]
    arg = {
        "v_dbinfo": v_dbinfo
    }
    print(f_priv_db_user_list(**arg))
