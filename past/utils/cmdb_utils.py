# -*- coding: utf-8 -*-
from plain_db.oracleob import OracleHelper

class CmdbUtils:

    @classmethod
    def get_cmdb(cls, cmdb_id):

        sql = f"SELECT * FROM t_cmdb WHERE cmdb_id = {cmdb_id}"
        return OracleHelper.select_dict(sql, one=True)

    @classmethod
    def get_cmdbs(cls, condition):

        sql = f""
        return OracleHelper.select(sql, one=False)
