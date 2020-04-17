
# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "SQLText"
]

from mongoengine import StringField, IntField

from .base import SQLCapturingDoc


@SQLCapturingDoc.need_collect()
class SQLText(SQLCapturingDoc):
    """sql文本信息"""

    dbid = IntField(null=True)
    sql_id = StringField(null=True)
    short_sql_text = StringField(null=True)
    longer_sql_text = StringField(null=True)

    meta = {
        "collection": "sqltext",
        "indexes": [
            "sql_id"
        ]
    }

    @classmethod
    def simple_capture(cls, **kwargs) -> str:
        sql_id: str = kwargs["sql_id"]

        return f"""
SELECT p.dbid,
       p.sql_id,
       dbms_lob.substr(p.sql_text,40,1) short_sql_text,
       dbms_lob.substr(p.sql_text,2000,1) longer_sql_text
FROM dba_hist_sqltext p
WHERE p.sql_id = '{sql_id}'
"""
