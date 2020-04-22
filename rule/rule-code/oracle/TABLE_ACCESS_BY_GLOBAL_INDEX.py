import re

from rule import const
from oracle_cmdb.capture.sqlplan import *


def code(rule, entries, **kwargs):

    if const.RULE_ENTRY_TICKET_DYNAMIC in entries:
        sql_plan_qs = kwargs["sql_plan_qs"]

        if sql_plan_qs.filter(
            operation="TABLE ACCESS",
            options=re.compile(r"BY GLOBAL INDEX ROWID", re.I)
        ).count():
            yield {}

    elif const.RULE_ENTRY_ONLINE_SQL_PLAN:
        task_record_id: int = kwargs["task_record_id"]
        schema_name: str = kwargs["schema_name"]

        ret = SQLPlan.aggregate(
            match_append={
                "task_record_id": task_record_id,
                "schema_name": schema_name,
                "operation": "TABLE ACCESS",
                "options": re.compile(r"BY GLOBAL INDEX ROWID", re.I)
            }
        )
        for i in ret:
            yield {
                "issue_desc": f"",
                "sql_id": i["_id"]["sql_id"],
                "plan_hash_value": i["_id"]["plan_hash_value"],
            }


code_hole.append(code)
