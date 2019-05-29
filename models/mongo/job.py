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
    id = ObjectIdField()
    name = StringField()
    cmdb_id = IntField()
    schema_name = StringField()
    status = IntField(help_text="0失败 1成功 2正在运行")
    create_time = DateTimeField()
    etl_date = DateTimeField()
    end_date = DateTimeField(null=True)
    operator_user = StringField()
    connect_name = StringField()
    record_id = StringField()
    exported = BooleanField()
    desc = EmbeddedDocumentField(EmbeddedJobDesc)
    score = IntField()

    meta = {
        "collection": "job"
    }
