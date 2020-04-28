# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleOnlineSQLTextIssueSingleAnalyse"
]

from typing import Generator, List

import rule.const
from ....issue.base import *
from .sql_text import *
from ....single_sql import SingleSQLForOnline
from ....cmdb import OracleCMDB
from ....plain_db import *
from rule.adapters import CMDBRuleAdapterSQL


@OracleOnlineIssue.need_collect()
class OracleOnlineSQLTextIssueSingleAnalyse(OracleOnlineSQLTextIssue):
    """sql文本问题(逐个分析)"""

    ENTRIES = (rule.const.RULE_ENTRY_ONLINE_SINGLE,)

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
        for single_sql in sqls:
            ret = CMDBRuleAdapterSQL(the_rule).run(
                entries=entries,

                cmdb=the_cmdb,
                single_sql=single_sql
            )
            docs = cls.pack_rule_ret_to_doc(the_rule, ret)
            cls.post_analysed(
                docs=docs,
                task_record_id=task_record_id,
                schema_name=schema_name
            )
            yield from docs

