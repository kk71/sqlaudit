
# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleSQLText"
]

from mongoengine import StringField, IntField

from .base import SQLCapturingDoc


@SQLCapturingDoc.need_collect()
class OracleSQLText(SQLCapturingDoc):
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
    def convert_sql_set_bulk_to_sql_filter(cls, sql_set_bulk):
        s = [f"p.sql_id = '{sql_id}'" for sql_id, _ in sql_set_bulk]
        return " or ".join(s)

    @classmethod
    def simple_capture(cls, **kwargs) -> str:
        filters: str = kwargs["filters"]

        return f"""
SELECT p.dbid,
       p.sql_id,
       dbms_lob.substr(p.sql_text,40,1) short_sql_text,
       dbms_lob.substr(p.sql_text,2000,1) longer_sql_text
FROM dba_hist_sqltext p
WHERE {filters}
"""
