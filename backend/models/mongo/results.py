# Author: kk.Fang(fkfkbill@gmail.com)

from mongoengine import StringField, IntField, FloatField, ListField, DictField,\
    ObjectIdField, DateTimeField
from backend.models.mongo.utils import BaseDoc


class Results(BaseDoc):
    _id = ObjectIdField()
    task_uuid = StringField(null=False)
    cmdb_id = IntField()
    schema_name = StringField()
    create_date = DateTimeField()
    etl_date = DateTimeField()
    ip_address = StringField()
    sid = StringField()
    record_id = StringField()
    rule_type = StringField()

    # dynamic collection, remain keys are rule_name(s)
    # "rule_name": {
    #     "records": [
    #         [returned values, ...(a function may return many values as tuple)], ...
    #     ],
    #     "scores": the score(a float) of this rule for the result
    # }

    meta = {
        "collection": "results"
    }
