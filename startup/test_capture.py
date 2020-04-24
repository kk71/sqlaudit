# Author: kk.Fang(fkfkbill@gmail.com)

from models import init_models
init_models()

from oracle_cmdb.cmdb import *
from oracle_cmdb.capture.base.sql import SQLCapturingDoc


def main():

    cmdb_id = 2526
    schema_name = "ISQLAUDIT"
    snap_s = 3086
    snap_e = 3109

    conn = OracleCMDB.build_connector_by_cmdb_id(cmdb_id)
    sql_set = SQLCapturingDoc.query_sql_set(conn, schema_name, snap_s, snap_e)
    print(f"{sql_set=}")

