# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleOnlineSQLTextIssue"
]

from copy import deepcopy
from typing import Generator, List

import rule.const
from models.sqlalchemy import *
from models.mongoengine import *
from oracle_cmdb.issue.sql import OracleOnlineSQLIssue
from oracle_cmdb.single_sql import SingleSQLForOnline
from oracle_cmdb.cmdb import OracleCMDB
from ....plain_db import *
from oracle_cmdb.capture import OracleSQLText
from rule.cmdb_rule import CMDBRule


class OracleOnlineSQLTextIssue(OracleOnlineSQLIssue):
    """sql文本问题"""

    ENTRIES = (rule.const.RULE_ENTRY_ONLINE_SQL_TEXT,)

    RELATED_CAPTURE = (OracleSQLText,)

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
        raise NotImplementedError

    @classmethod
    def simple_analyse(cls, **kwargs) -> Generator[
            "OracleOnlineSQLTextIssue", None, None]:

        task_record_id: int = kwargs["task_record_id"]
        cmdb_id: int = kwargs["cmdb_id"]
        schema_name: str = kwargs["schema_name"]

        rule_jar: [CMDBRule] = cls.generate_rule_jar(
            cmdb_id,
            task_record_id=task_record_id,
            schema_name=schema_name
        )
        sqls: List[SingleSQLForOnline] = [
            SingleSQLForOnline.gen_from_sql_text(i)
            for i in list(cls.get_sql_qs(task_record_id, schema_name))
        ]
        with make_session() as session:
            the_cmdb = session.query(
                OracleCMDB).filter_by(cmdb_id=cmdb_id).first()
            cmdb_connector = the_cmdb.build_connector()
            for the_rule in rule_jar:
                yield from cls._single_rule(
                    the_rule=the_rule,
                    sqls=deepcopy(sqls),
                    cmdb_connector=cmdb_connector,
                    entries=rule_jar.entries,
                    the_cmdb=the_cmdb,
                    task_record_id=task_record_id,
                    schema_name=schema_name
                )

    @classmethod
    def get_sql_qs(
            cls,
            task_record_id: int,
            schema_name: str) -> mongoengine_qs:
        """获取采集任务的sql"""
        return OracleSQLText.filter(
            task_record_id=task_record_id,
            schema_name=schema_name
        )
