import re
import sys

print(sys.path)

from rule import const
from oracle_cmdb.ticket.sql_plan import OracleTicketSQLPlan
from oracle_cmdb.capture import SQLPlan


def code(rule, entries, **kwargs):

    nested_loop = {
        "$or": [
            {"operation": re.compile(r"NESTED LOOP")},
            {"operation": re.compile(r"FILTER")}
        ]
    }

    if const.RULE_ENTRY_TICKET_DYNAMIC in entries:
        statement_id: str = kwargs["statement_id"]
        ret = OracleTicketSQLPlan.objects.aggregate(
            {
                "$match": {
                    "statement_id": statement_id,
                    **nested_loop
                }
            },
            {
                "$group": {
                    "_id": {
                        "statement_id": "$statement_id",
                        "plan_id": "$plan_id",
                        "object_name": "$object_name",
                        "object_type": "$object_type",
                    },
                    "count": {"$sum": 1}
                }
            }
        )
        for d in ret:
            if d["count"] >= rule.gip("loop_num"):
                yield {
                    "object_name": d["_id"]["object_name"],
                    "object_type": d["_id"]["object_type"]
                }

    elif const.RULE_ENTRY_ONLINE_SQL_PLAN in entries:
        schema_name: str = kwargs["schema_name"]
        task_record_id: int = kwargs["task_record_id"]

        ret = SQLPlan.objects.aggregate(
            {
                "$match": {
                    "task_record_id": task_record_id,
                    "schema_name": schema_name,
                    **nested_loop
                }
            },
            {
                "$group": {
                    "_id": {
                        "sql_id": "$sql_id",
                        "plan_hash_value": "$plan_hash_value",
                        "object_name": "$object_name",
                        "object_type": "$object_type",
                    },
                    "count": {"$sum": 1}
                }
            }
        )
        for d in ret:
            if d["count"] >= rule.gip("loop_num"):
                yield {
                    "count": d["count"],
                    **d["_id"]
                }


code_hole.append(code)


