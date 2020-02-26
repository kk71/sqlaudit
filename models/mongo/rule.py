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

    def get_object_name(self, record, obj_info_type) -> Union[str, None]:
        """获取一条record数据的对象名"""
        key_index = None
        for i, parm in enumerate(self.output_parms):
            if obj_info_type == const.OBJ_RULE_TYPE_TABLE \
                    and "表名" in parm["parm_desc"]:
                key_index = i
                break
            elif obj_info_type == const.OBJ_RULE_TYPE_INDEX \
                    and "索引名" in parm["parm_desc"]:
                key_index = i
                break
            elif obj_info_type == const.OBJ_RULE_TYPE_SEQ \
                    and "序列名" in parm["parm_desc"]:
                key_index = i
                break
        if key_index is not None:
            the_obj_name = record[key_index]
            if "." in the_obj_name:
                # 排除可能存在schema_name.object_name的情况
                splited_obj_name = [i for i in the_obj_name.split(".") if i]
                if len(splited_obj_name) == 2:
                    the_obj_name = splited_obj_name[-1]
            return the_obj_name

    @classmethod
    def filter_enabled(cls, *args, **kwargs):
        """返回当前启用的规则查询集"""
        return cls.objects.filter(*args,
                                  rule_status=const.RULE_STATUS_ON,
                                  db_type=const.DB_ORACLE,  # TODO 目前只返回oracle类型的规则！！！
                                  **kwargs)
