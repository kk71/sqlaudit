# Author: kk.Fang(fkfkbill@gmail.com)

from mongoengine import IntField, StringField, ObjectIdField, DateTimeField,\
    BooleanField, DictField, EmbeddedDocument, EmbeddedDocumentField, FloatField

from .utils import BaseDoc


class EmbeddedJobDesc(EmbeddedDocument):
    db_ip = StringField()
    port = StringField()
    owner = StringField(help_text="schema")
    rule_type = StringField()
    instance_name = StringField()
    capture_time_start = DateTimeField()
    capture_time_end = DateTimeField()


class Job(BaseDoc):
    id = ObjectIdField(db_field="_id")
    name = StringField()
    cmdb_id = IntField()
    status = IntField(help_text="0失败 1成功 2正在运行")
    create_time = DateTimeField()
    etl_date = DateTimeField(db_field="ETL_DATE")
    end_time = DateTimeField(null=True)
    operator_user = StringField()
    connect_name = StringField()
    record_id = StringField()
    exported = BooleanField(default=False)
    desc = EmbeddedDocumentField(EmbeddedJobDesc)
    score = FloatField(help_text="only some of Job objects contains score")

    meta = {
        "collection": "job"
    }
