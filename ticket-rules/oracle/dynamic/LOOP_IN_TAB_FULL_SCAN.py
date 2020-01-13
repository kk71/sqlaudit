import re

from mongoengine import Q

from utils.mongo_utils import *
from models.mongo import ObjTabInfo


def code(rule, **kwargs):
    sql_plan_qs = kwargs["sql_plan_qs"]
    mongo_connector = kwargs["mongo_connector"]
    schema_name = kwargs["schema_name"]

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
            return None, []
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
            each_ar_found = tmp.find({
                "statement_id": ar["_id"]["statement_id"],
                "plan_id": ar["_id"]["plan_id"],
                "$and": [
                    {"the_ud": ar["MAXID"]},
                    {"the_id": {"$ne": 2}}
                ],
                "username": schema_name,
                "operation": "TABLE ACCESS",
                "options": "FULL"
            })
            for i in each_ar_found:
                tab = ObjTabInfo.objects(num_row__gt=rule.gip("table_row_num"),
                                         owner=i["object_owner"],
                                         table_name=i["object_name"],
                                         ).first()
                if tab:
                    return -rule.weight, [
                        i["statement_id"],
                        i["plan_id"],
                        i["object_name"],
                        i["the_id"],
                        i["cost"],
                        i["count"]
                    ]
    return None, []


code_hole.append(code)
