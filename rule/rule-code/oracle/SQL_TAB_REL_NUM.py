from oracle_cmdb.capture import SQLPlan


def code(rule, entries, **kwargs):
    task_record_id: int = kwargs["task_record_id"]
    schema_name: str = kwargs["schema_name"]

    ret = SQLPlan.objects.aggregate(
        {
            "$match": {
                "task_record_id": task_record_id,
                "schema_name": schema_name,
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
