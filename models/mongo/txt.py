# Author: kk.Fang(fkfkbill@gmail.com)

from mongoengine import IntField, StringField, ObjectIdField, DateTimeField, \
    BooleanField, DictField

from .utils import BaseDoc


class SQLText(BaseDoc):
    _id = ObjectIdField()
    dbid = IntField(db_field="DBID", help_text="select dbid from v$database;")
    ip_address = StringField(db_field="IPADDR")
    db_sid = StringField(db_field="DB_SID")
    schema = StringField(db_field="USERNAME")
    etl_date = DateTimeField(db_field="ETL_DATE")
    record_id = StringField()
    cmdb_id = IntField()
    sql_id = StringField(db_field="SQL_ID", help_text="the hash of the sql text")
    sql_text = StringField(db_field="SQL_TEXT_DETAIL")

    meta = {
        "collection": "sqltext"
    }
