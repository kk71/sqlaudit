# Author: kk.Fang(fkfkbill@gmail.com)

from mongoengine import IntField, StringField, ObjectIdField, DateTimeField,\
    BooleanField, DictField, EmbeddedDocument, EmbeddedDocumentField

from .utils import BaseDoc


class EmbeddedJobDesc(EmbeddedDocument):
    db_ip = StringField()
    port = StringField()
    owner = StringField()
    rule_type = StringField()
    instance_name = StringField()
    capture_time_start = DateTimeField()
    capture_time_end = DateTimeField()


class Job(BaseDoc):
    _id = ObjectIdField()
    cmdb_id = IntField()
    schema = StringField()
    status = IntField()
    create_time = DateTimeField()
    etl_time = DateTimeField()
    end_time = DateTimeField()
    operator_user = StringField()
    connect_name = StringField()
    record_id = StringField()
    exported = BooleanField()
    desc = EmbeddedDocumentField(EmbeddedJobDesc)
    score = IntField()

    meta = {
        "collection": "job"
    }
