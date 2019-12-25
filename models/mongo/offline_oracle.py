# Author: kk.Fang(fkfkbill@gmail.com)

from mongoengine import StringField, IntField, DateTimeField, FloatField

from .offline import TicketSQLPlan


class OracleTicketSQLPlan(TicketSQLPlan):
    """oracle工单动态审核产生的执行计划"""

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
            "statement_id",
            "timestamp"
        ]
    }

    @classmethod
    def add_from_dict(cls,
                      work_list_id: int,
                      cmdb_id: int,
                      schema_name: str,
                      list_of_plan_dicts: list) -> list:
        docs = []
        for one_dict in list_of_plan_dicts:
            doc = cls(
                **one_dict,
                work_list_id=work_list_id,
                cmdb_id=cmdb_id,
                schema_name=schema_name,
            )
            doc.from_dict(one_dict)
            docs.append(doc)
        cls.objects.insert(docs)


