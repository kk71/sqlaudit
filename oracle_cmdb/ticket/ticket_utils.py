# Author: kk.Fang(fkfkbill@gmail.com)

from typing import Union
from plain_db.oracleob import OracleOB


def check_cmdb_privilege(cmdb: Union[int]) -> tuple:
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
