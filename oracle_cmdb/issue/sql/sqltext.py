# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleOnlineSQLTextIssue"
]

from typing import Generator, List, NoReturn

from mongoengine import StringField

import rule.const
from models.sqlalchemy import *
from models.mongoengine import *
from ..base import OracleOnlineIssue
from ..sql import OracleOnlineSQLIssue
from ...single_sql import SingleSQLForOnline
from ...cmdb import OracleCMDB
from ...capture import OracleSQLText
from rule.cmdb_rule import CMDBRule
from rule.adapters import CMDBRuleAdapterSQL


@OracleOnlineIssue.need_collect()
class OracleOnlineSQLTextIssue(OracleOnlineSQLIssue):
    """sql文本问题"""

    short_sql_text = StringField(null=True)
    longer_sql_text = StringField(null=True)

    ENTRIES = (rule.const.RULE_ENTRY_ONLINE_SQL_TEXT,)

    @classmethod
    def simple_analyse(cls, **kwargs) -> Generator["OracleOnlineSQLTextIssue", None, None]:
        task_record_id: int = kwargs["task_record_id"]
        cmdb_id: int = kwargs["cmdb_id"]
        schema_name: str = kwargs["schema_name"]

        rule_jar: [CMDBRule] = cls.generate_rule_jar(
            cmdb_id,
            task_record_id=task_record_id,
            append_data = {"schema_name": schema_name}
        )
        sqltext = list(cls.get_sql_qs(task_record_id, schema_name))
        with make_session() as session:
            the_cmdb = session.query(
                OracleCMDB).filter_by(cmdb_id=cmdb_id).first()
            for the_rule in rule_jar:
                for a_sqltext in sqltext:
                    ret = CMDBRuleAdapterSQL(the_rule).run(
                        entries=rule_jar.entries,

                        cmdb=the_cmdb,
                        single_sql=SingleSQLForOnline.gen_from_sql_text(a_sqltext),
                        sql_id=a_sqltext.sql_id
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
    def get_sql_qs(
            cls,
            task_record_id: int,
            schema_name: str) -> mongoengine_qs:
        """获取采集任务的sql_id"""
        return OracleSQLText.filter(
            task_record_id=task_record_id,
            schema_name=schema_name
        )

    @classmethod
    def post_analysed(cls, **kwargs) -> NoReturn:
        docs: List["OracleOnlineSQLTextIssue"] = kwargs["docs"]
        short_sql_text: str = kwargs["short_sql_text"]
        longer_sql_text: str = kwargs["longer_sql_text"]

        super().post_analysed(**kwargs)
        for doc in docs:
            doc.short_sql_text = short_sql_text
            doc.longer_sql_text = longer_sql_text

