# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleOnlineSQLTextIssue"
]

from typing import Generator, List, NoReturn

from mongoengine import StringField

import rule.const
from models.sqlalchemy import *
from ..cmdb import OracleCMDB
from .base import OracleOnlineIssue
from .sql import OracleOnlineSQLIssue
from ..single_sql import SingleSQLForOnline
from rule.cmdb_rule import CMDBRule
from rule.adapters import CMDBRuleAdapterSQL
from utils.log_utils import *
from ..capture.sqltext import SQLText


@OracleOnlineIssue.need_collect()
class OracleOnlineSQLTextIssue(OracleOnlineSQLIssue):
    """sql文本问题"""

    short_sql_text = StringField(null=True)
    longer_sql_text = StringField(null=True)

    ENTRIES = (rule.const.RULE_ENTRY_ONLINE_SQL_TEXT,)

    @classmethod
    def _single_rule_analyse(
            cls,
            the_rule: CMDBRule,
            entries: [str],
            the_cmdb: OracleCMDB,
            task_record_id: int,
            schema_name: str,
            **kwargs) -> Generator["OracleOnlineSQLTextIssue", None, None]:
        sqltext: List[SQLText] = kwargs["sqltext"]

        for a_sqltext in sqltext:
            ret = CMDBRuleAdapterSQL(the_rule).run(
                entries=entries,

                cmdb=the_cmdb,
                single_sql=SingleSQLForOnline.gen_from_sql_text(a_sqltext),
                task_record_id=task_record_id,
                schema_name=schema_name,
            )
            docs = cls.pack_rule_ret_to_doc(the_rule, ret)
            cls.post_analysed(
                docs=docs,
                task_record_id=task_record_id,
                schema_name=schema_name,
                sql_id=a_sqltext.sql_id,
                short_sql_text=a_sqltext.short_sql_text,
                longer_sql_text=a_sqltext.longer_sql_text
            )
            yield from docs

    @classmethod
    def simple_analyse(cls, **kwargs) -> Generator["OracleOnlineSQLTextIssue", None, None]:
        task_record_id: int = kwargs["task_record_id"]
        cmdb_id: int = kwargs["cmdb_id"]
        schema_name: str = kwargs["schema_name"]

        rule_jar: [CMDBRule] = cls.generate_rule_jar()
        entries = cls.inherited_entries()
        sqltext = list(cls.get_sql_qs(task_record_id, schema_name))
        with make_session() as session:
            the_cmdb = session.query(
                OracleCMDB).filter_by(cmdb_id=cmdb_id).first()
            with grouped_count_logger(
                    cls.__doc__, item_type_name="rule") as counter:
                for the_rule in rule_jar:
                    docs = list(cls._single_rule_analyse(
                        the_rule=the_rule,
                        entries=entries,
                        the_cmdb=the_cmdb,
                        task_record_id=task_record_id,
                        schema_name=schema_name,
                        sqltext=sqltext
                    ))
                    counter(the_rule.name, len(docs))
                    yield from docs

    @classmethod
    def post_analysed(cls, **kwargs) -> NoReturn:
        docs: List["OracleOnlineSQLTextIssue"] = kwargs["docs"]
        short_sql_text: str = kwargs["short_sql_text"]
        longer_sql_text: str = kwargs["longer_sql_text"]

        for doc in docs:
            doc.short_sql_text = short_sql_text
            doc.longer_sql_text = longer_sql_text

