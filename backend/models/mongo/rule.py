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
    rule_type_detail = StringField()  # 极少数包含此字段
    solution = ListField()
    weight = FloatField()
    obj_info_type = StringField()  # 极少数包含此字段
    output_datas = ListField()  # 极少数包含此字段

    meta = {
        "collection": "rule",
        'indexes': [
            {'fields': ("db_type", "db_model", "rule_name"), 'unique': True}
        ]
    }

    def get_3_key(self) -> tuple:
        return self.db_type, self.db_model, self.rule_name
