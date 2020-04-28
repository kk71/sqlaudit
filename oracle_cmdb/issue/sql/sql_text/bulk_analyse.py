# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleOnlineSQLTextIssueBulkAnalyse"
]

from copy import deepcopy
from typing import Generator, List

import settings
import rule.const
from ....issue.base import *
from .sql_text import *
from ....single_sql import SingleSQLForOnline
from ....cmdb import OracleCMDB
from ....plain_db import *
from rule.adapters import CMDBRuleAdapterSQL


@OracleOnlineIssue.need_collect()
class OracleOnlineSQLTextIssueBulkAnalyse(OracleOnlineSQLTextIssue):
    """sql文本问题"""

    ENTRIES = (rule.const.RULE_ENTRY_ONLINE_BULK,)

    SQL_ID_BULK_NUM = settings.ORACLE_SQL_ID_BULK_NUM

    @classmethod
    def single_sql_in_bulk(
            cls,
            single_sqls: List[SingleSQLForOnline]) -> Generator[
                List[SingleSQLForOnline], None, None]:
        single_sqls = deepcopy(single_sqls)
        while single_sqls:
            yield single_sqls[-cls.SQL_ID_BULK_NUM:]
            del single_sqls[-cls.SQL_ID_BULK_NUM:]

    @classmethod
    def _single_rule(cls,
                     the_rule,
                     sqls: List[SingleSQLForOnline],
                     cmdb_connector: OraclePlainConnector,
                     entries: List[str],
                     the_cmdb: OracleCMDB,
                     task_record_id: int,
                     schema_name: str,
                     **kwargs) -> Generator["OracleOnlineSQLTextIssue", None, None]:

        for single_sqls_bulk in cls.single_sql_in_bulk(sqls):
            ret = CMDBRuleAdapterSQL(the_rule).run(
                entries=entries,

                cmdb=the_cmdb,
                sqls=single_sqls_bulk,
                cmdb_connector=cmdb_connector
            )
            docs = cls.pack_rule_ret_to_doc(the_rule, ret)
            cls.post_analysed(
                docs=docs,
                task_record_id=task_record_id,
                schema_name=schema_name
            )
            yield from docs

