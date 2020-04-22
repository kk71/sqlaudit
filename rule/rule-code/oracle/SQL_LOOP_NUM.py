import re

from oracle_cmdb.ticket.sql_plan import OracleTicketSQLPlan


def code(rule, entries, **kwargs):
    statement_id: str = kwargs["statement_id"]

    agg_ret = OracleTicketSQLPlan.objects.aggregate(
        {
            "$match": {
                "statement_id": statement_id,
                "$or": [
                    {"operation": re.compile(r"NESTED LOOP")},
                    {"operation": re.compile(r"FILTER")}
                ]
            }
        },
        {
            "$group": {
                "_id": {
                    "statement_id": "$statement_id",
                    "plan_id": "$plan_id"
                },
                "count": {"$sum": 1}
            }
        }
    )
    for r in agg_ret:
        if r["count"] > rule.gip("loop_num"):
            return -rule.weight, [
                statement_id,
                r["_id"]["plan_id"],
                r["count"]
            ]
    return None, []


code_hole.append(code)


