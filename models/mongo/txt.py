# Author: kk.Fang(fkfkbill@gmail.com)

from mongoengine import IntField, StringField, ObjectIdField, DateTimeField, \
    BooleanField, DictField

from .utils import BaseDoc


class SQLText(BaseDoc):
    _id = ObjectIdField()
    dbid = IntField(help_text="select dbid from v$database;")
    ip_address = StringField()
    db_sid = StringField()
    schema = StringField()
    etl_date = DateTimeField()
    record_id = StringField()
    cmdb_id = IntField()
    sql_id = StringField(help_text="the hash of the sql text")
    sql_text = StringField()

    meta = {
        "collection": "sqltext"
    }
