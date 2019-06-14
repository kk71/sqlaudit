# Author: kk.Fang(fkfkbill@gmail.com)

from mongoengine import StringField, IntField, ObjectIdField, DateTimeField, Q

from .utils import BaseDocRecordID


class Results(BaseDocRecordID):
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
    # for object rules are like:
    # "rule_name": {
    #     "records": [
    #         [returned values, ...(a function may return many values as tuple)], ...
    #     ],
    #     "scores": float
    # }
    #
    # for sql(text, plan and statistics) rules:
    # "rule_name": {
    #     sqls: [
    #         {
    #             "sql_id": str,
    #             "plan_hash_value" : int,
    #             "schema": str,  # TODO add this key
    #             "sql_text" : str,
    #             "stat" : {
    #                 "CPU_TIME_DELTA" : 4.5589,
    #                 "PER_CPU_TIME" : 4.5589,
    #                 "DISK_READS_DELTA" : 0,
    #                 "PER_DISK_READS" : 0,
    #                 "ELAPSED_TIME_DELTA" : 4.5705,
    #                 "PER_ELAPSED_TIME" : 4.5705,
    #                 "BUFFER_GETS_DELTA" : 1580111,
    #                 "PER_BUFFER_GETS" : 1580111,
    #                 "EXECUTIONS_DELTA" : 1
    #             },
    #             "obj_info" : dict,
    #             "obj_name" : str,
    #             "cost" : float,
    #             "count" : int
    #         }, ...
    #     ],
    #     scores: float
    # }

    meta = {
        "collection": "results"
    }
