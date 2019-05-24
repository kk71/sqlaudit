# Author: kk.Fang(fkfkbill@gmail.com)

from mongoengine import IntField, StringField, ObjectIdField, DateTimeField,\
    BooleanField, DictField

from .utils import BaseDoc


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
    desc = DictField(field=["db_ip",            # str
                            "port",             # str
                            "owner",            # str
                            "rule_type",        # str
                            "instance_name",    # str
                            "capture_time_s",   # datetime
                            "capture_time_e"    # datetime
                            ])
    score = IntField()

    meta = {
        "collection": "job"
    }
