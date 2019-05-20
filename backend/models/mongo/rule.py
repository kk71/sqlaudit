# Author: kk.Fang(fkfkbill@gmail.com)

from mongoengine import StringField, IntField, FloatField, ListField, DictField, ObjectIdField
from backend.models.mongo.utils import BaseDoc


class Rule(BaseDoc):
    """规则仓库"""
    db_type = StringField(required=True)
    db_model = StringField()
    exclude_obj_type = ListField()
    input_parms = ListField()
    max_score = IntField()
    output_parms = ListField()
    rule_desc = StringField()
    rule_name = StringField(required=True)
    rule_complexity = StringField()
    rule_cmd = StringField()
    rule_status = StringField()
    rule_summary = StringField()
    rule_type = StringField()
    solution = ListField()
    weight = FloatField()

    meta = {
        "collection": "rule"
    }
