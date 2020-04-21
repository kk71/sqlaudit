import re

from mongoengine import Q

from utils.mongo_utils import *
# from models.mongo import ObjTabInfo, OracleTicketSQLPlan


def code(rule, entries, **kwargs):

    sql_plan_qs = kwargs["sql_plan_qs"]
    mongo_connector = kwargs["mongo_connector"]

    plans = sql_plan_qs.filter(
        Q(operation=re.compile(r"NESTED LOOP")) |
        Q(operation=re.compile(r"FILTER"))
    )
    with temp_collection(mongo_connector, rule.name) as tmp:
        to_insert = [
            i.to_dict(
                iter_if=lambda k, v: k in (
                    "statement_id",
                    "plan_id",
                    "object_name",
                    "the_id",
                    "parent_id",
                    "operation",
                    "options",
                    "username"
                )
            ) for i in plans
        ]
        if to_insert:
            tmp.insert_many(to_insert)
        else:
            return
        aggregate_result = tmp.aggregate([
            {
                "$group": {
                    "_id": {
                        "parent_id": "$parent_id",
                        "statement_id": "$statement_id",
                        "plan_id": "$plan_id"
                    },
                    "max_id": {"$max": "$the_id"}
                }
            }
        ])
        for ar in aggregate_result:
            plans_after_aggregation = OracleTicketSQLPlan.objects(
                Q(the_id=ar["max_id"]) & Q(the_id__ne=2),
                statement_id=ar["_id"]["statement_id"],
                plan_id=ar["_id"]["plan_id"],
                operation="TABLE ACCESS",
                options="FULL"
            )
            for paa in plans_after_aggregation:
                a_tab = ObjTabInfo.objects(
                    Q(num_rows__gt=rule.gip("table_row_num")) |
                    Q(phy_size_mb__gt=rule.gip("table_phy_size")),
                    owner=paa.owner,
                    ip_address=rule.gip("ip_addr"),
                    sid=rule.gip("sid"),
                    table_name=paa.object_name,
                ).first()
                if a_tab:
                    yield {}


code_hole.append(code)
