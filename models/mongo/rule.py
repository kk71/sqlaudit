# Author: kk.Fang(fkfkbill@gmail.com)

from typing import Union

from mongoengine import StringField, IntField, FloatField, ListField, \
    ObjectIdField, DynamicField

from .utils import BaseDoc
from utils import const


class Rule(BaseDoc):
    """规则仓库"""
    _id = ObjectIdField()
    db_type = StringField(required=True)
    db_model = StringField()
    exclude_obj_type = DynamicField(default=list)
    input_parms = ListField()
    max_score = IntField()
    output_parms = ListField()
    rule_desc = StringField(required=True)
    rule_name = StringField(required=True)
    rule_complexity = StringField()
    rule_cmd = StringField()
    rule_status = StringField()
    rule_summary = StringField()
    rule_type = StringField(required=True)
    rule_type_detail = StringField()  # 极少数包含此字段
    solution = ListField()
    weight = FloatField()
    obj_info_type = StringField(null=True)  # 类型为OBJ的规则，指明适用哪种OBJ
    output_datas = ListField()  # 极少数包含此字段
    sql_type = StringField(choices=const.ALL_SQL_TYPE)  # 线下审核SQL的类型
    ddl_type = StringField(choices=const.ALL_DDL_TYPE)  # 线下审核DDL的详细分类

    meta = {
        "collection": "rule",
        'indexes': [
            {'fields': ("db_type", "db_model", "rule_name"), 'unique': True}
        ]
    }

    def get_3_key(self) -> tuple:
        return self.db_type, self.db_model, self.rule_name

    def get_table_name(self, record) -> Union[str, None]:
        """获取一条record数据的表名"""
        table_name_key_index = None
        for i, parm in enumerate(self.output_parms):
            if "表名" in parm["parm_desc"]:
                table_name_key_index = i
                break
        if table_name_key_index is not None:
            return record[table_name_key_index]

    @classmethod
    def filter_enabled(cls, *args, **kwargs):
        """返回当前启用的规则查询集"""
        return cls.objects.filter(*args,
                                  rule_status=const.RULE_STATUS_ON,
                                  db_type=const.DB_ORACLE,  # TODO 目前只返回oracle类型的规则！！！
                                  **kwargs)
