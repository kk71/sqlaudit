# Author: kk.Fang(fkfkbill@gmail.com)

from mongoengine import IntField, StringField, ObjectIdField, DateTimeField, \
    BooleanField, DictField

from .utils import BaseDoc


class SQLText(BaseDoc):
    _id = ObjectIdField()
    dbid = IntField("DBID", help_text="select dbid from v$database;")
    ip_address = StringField("IPADDR")
    db_sid = StringField("DB_SID")
    schema = StringField("USERNAME")
    etl_date = DateTimeField("ETL_DATE")
    record_id = StringField()
    cmdb_id = IntField()
    sql_id = StringField("SQL_ID", help_text="the hash of the sql text")
    sql_text = StringField("SQL_TEXT_DETAIL")

    meta = {
        "collection": "sqltext"
    }
