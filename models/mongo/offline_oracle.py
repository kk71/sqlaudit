# Author: kk.Fang(fkfkbill@gmail.com)

from mongoengine import StringField, IntField, DateTimeField, FloatField

from .offline import TicketSQLPlan, TicketSubResult


class OracleTicketSubResult(TicketSubResult):
    """oracle的子工单"""
    schema_name = StringField()

    meta = {
        'indexes': [
            "schema_name",
        ]
    }


class OracleTicketSQLPlan(TicketSQLPlan):
    """oracle的工单动态审核产生的执行计划"""
    schema_name = StringField()

    # 以下都是oracle的plan_table返回的数据结构
    statement_id = StringField()
    plan_id = IntField()
    timestamp = DateTimeField()
    remarks = StringField()
    operation = StringField()
    options = StringField()
    object_node = StringField()
    object_owner = StringField()
    object_name = StringField()
    object_alias = StringField()
    object_instance = IntField()
    object_type = StringField()
    optimizer = StringField()
    search_columns = IntField()
    # plan_table里的字段就叫做id，这里为了避免python冲突改名叫the_id
    the_id = IntField()
    parent_id = IntField()
    depth = IntField()
    position = IntField()
    cost = IntField()
    cardinality = IntField()
    bytes = IntField()
    other_tag = StringField()
    partition_start = StringField()
    partition_stop = StringField()
    partition_id = IntField()
    other = IntField()
    other_xml = StringField()
    distribution = StringField()
    cpu_cost = FloatField()
    io_cost = FloatField()
    temp_space = FloatField()
    access_predicates = StringField()
    filter_predicates = StringField()
    projection = StringField()
    time = FloatField()
    qblock_name = StringField()

    meta = {
        "collection": "oracle_ticket_sql_plan",
        'indexes': [
            "schema_name",
            "statement_id",
            "timestamp"
        ]
    }

    @classmethod
    def add_from_dict(cls,
                      work_list_id: int,
                      cmdb_id: int,
                      schema_name: str,
                      list_of_plan_dicts: list):
        docs = []
        for one_dict in list_of_plan_dicts:
            if "id" in one_dict.keys():
                # oracle的plan_tab里字段叫id，为了避免混淆改名the_id
                one_dict["the_id"] = one_dict.pop("id")
            doc = cls(
                work_list_id=work_list_id,
                cmdb_id=cmdb_id,
                schema_name=schema_name,
            )
            doc.from_dict(one_dict, iter_if=lambda k, v: k not in ("other_xml",))
            docs.append(doc)
        cls.objects.insert(docs)
