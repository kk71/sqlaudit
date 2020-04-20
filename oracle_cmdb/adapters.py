# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "CMDBRuleAdapterSQLOracleOnline"
]

from rule.adapters import *
from utils.single_sql import *
from oracle_cmdb.cmdb import OracleCMDB
from oracle_cmdb.capture import SQLText
from parsed_sql.parsed_sql import *


class CMDBRuleAdapterSQLOracleOnline(CMDBRuleAdapterSQL):
    """为oracle纳管库线上审核设计的适配器"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def run(self,
            entries: [str],
            cmdb: OracleCMDB,
            single_sql: SQLText = None,
            sqls: [SingleSQL] = None,
            cmdb_connector=None,
            sql_plan_qs=None,
            schema_name: str = None,
            task_record_id: int = None,
            statement_id: str = None,
            sql_id: str = None
            ):
        single_sql = ParsedSQL(single_sql.longer_sql_text)
