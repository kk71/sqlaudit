# Author: kk.Fang(fkfkbill@gmail.com)

from mongoengine import StringField, IntField, ObjectIdField, DateTimeField, DictField

from utils import const
from .utils import BaseDocRecordID


class Results(BaseDocRecordID):
    _id = ObjectIdField()
    task_uuid = StringField(null=False)
    cmdb_id = IntField()
    schema_name = StringField()
    create_date = DateTimeField()
    etl_date = DateTimeField()
    capture_time_start = DateTimeField()
    capture_time_end = DateTimeField()
    ip_address = StringField()
    sid = StringField()
    record_id = StringField()
    rule_type = StringField()
    score = DictField()  # 内存放数据同StatsSchemaRate.score_rule_type[rule_type]

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
        "collection": "results",
        "indexes": [
            "cmdb_id",
            "task_uuid",
            "schema_name",
            "create_date",
            "rule_type"
        ]
    }

    def deduplicate_output(self, session, task_uuid, rule_name: str):
        """
        去重的输出分析结果
        :param session:
        :param task_uuid:
        :param rule_name:
        :return:
        """
        from models.oracle import CMDB
        from models.mongo import Rule

        cmdb = session.query(CMDB).filter_by(cmdb_id=self.cmdb_id).first()
        rule = Rule.filter_enabled(
            rule_name=rule_name,
            rule_type=self.rule_type,
            db_model=cmdb.db_model
        ).first()
        result = Results.objects(task_uuid=task_uuid).first()
        rule_dict_in_rst = getattr(result, rule_name)
        records = []
        columns = []
        try:
            if rule.rule_type == const.RULE_TYPE_OBJ:
                columns = [i["parm_desc"] for i in rule.output_parms]
                for original_record in rule_dict_in_rst.get("records", []):
                    converted_record = []
                    for i in original_record:
                        if isinstance(i, (tuple, list)):
                            converted_record.append(", ".join([
                                str(aaa) if not isinstance(aaa, (str, int, float))
                                else aaa
                                for aaa in i]))
                        else:
                            converted_record.append(i)
                    records.append(dict(zip(columns, converted_record)))

            elif rule.rule_type in [const.RULE_TYPE_SQLPLAN,
                                    const.RULE_TYPE_SQLSTAT]:
                for sql_dict in rule_dict_in_rst["sqls"]:
                    if sql_dict.get("obj_name", None):
                        obj_name = sql_dict["obj_name"]
                    else:
                        obj_name = "空"
                    if sql_dict.get("cost", None):
                        cost = sql_dict["cost"]
                    else:
                        cost = "空"
                    if sql_dict.get("stat", None):
                        count = sql_dict["stat"].get("ts_cnt", "空")
                    else:
                        count = "空"
                    records.append({
                        "SQL ID": sql_dict["sql_id"],
                        "SQL文本": sql_dict["sql_text"],
                        "执行计划哈希值": sql_dict["plan_hash_value"],
                        "对象名": obj_name,
                        "Cost": cost,
                        "计数": count
                    })
                if records:
                    columns = list(records[0].keys())

            elif rule.rule_type == const.RULE_TYPE_TEXT:
                records = [{
                    "SQL ID": i["sql_id"],
                    "SQL文本": i["sql_text"]
                } for i in rule_dict_in_rst["sqls"]]
                if records:
                    columns = list(records[0].keys())
        except:
            print("Rule types are other")
            pass
        return {
            "columns": columns,
            # "records": reduce(lambda x, y: x if y in x else x + [y], [[], ] + records),
            "records": records,
            "rule": rule
        }
