import oracle_cmdb.const
from oracle_cmdb.capture import OracleSQLPlan


def code(rule, entries, **kwargs):
    task_record_id: int = kwargs["task_record_id"]
    schema_name: str = kwargs["schema_name"]

    ret = OracleSQLPlan.aggregate(
        {
            "$match": {
                "task_record_id": task_record_id,
                "schema_name": schema_name,
                "two_days_capture": oracle_cmdb.const.SQL_TWO_DAYS_CAPTURE_TODAY,
                "object_type": "TABLE"
            }
        },
        {
            "$group": {
                "_id": {
                    "sql_id": "$sql_id",
                    "plan_hash_value": "$plan_hash_value",
                    "object_type": "$object_type"
                },
                "count": {"$sum": 1}
            }
        }
    )
    for d in ret:
        if d["count"] >= rule.gip("tab_num"):
            yield {
                "sql_id": d["_id"]["sql_id"],
                "plan_hash_value": d["_id"]["plan_hash_value"],
                "tab_num": d["count"]
            }


code_hole.append(code)
