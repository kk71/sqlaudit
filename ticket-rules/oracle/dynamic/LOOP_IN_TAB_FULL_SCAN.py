# Author: kk.Fang(fkfkbill@gmail.com)

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
        tmp.insert_many([
            i.to_dict(
                iter_if=lambda k, v: k in (
                    "statement_id",
                    "plan_hash_value",
                    "object_name",
                    "the_id",
                    "parent_id",
                    "operation",
                    "options",
                    "username"
                )
            ) for i in plans
        ])
        aggregate_result = tmp.aggregate([
            {
                "$group": {
                    "_id": {
                        "parent_id": "$parent_id",
                        "statement_id": "$statement_id",
                        "plan_hash_value": "$plan_hash_value"
                    },
                    "max_id": {"$max": "$the_id"}
                }
            }
        ])
        for ar in aggregate_result:
            each_ar_found = tmp.find({
                "statement_id": ar["_id"]["statement_id"],
                "plan_hash_value": ar["_id"]["plan_hash_value"],
                "$and": [
                    {"the_ud": ar["MAXID"]},
                    {"the_id": {"$ne": 2}}
                ],
                "username": schema_name,
                "operation": "TABLE ACCESS",
                "options": "FULL"
            })
            for i in each_ar_found:
                tab = ObjTabInfo.objects(
                    Q(num_row__gt=rule.gip("table_row_num")) |
                    Q(phy_size_mb__gt=rule.gip("table_phy_size")),
                    owner=i["object_owner"],
                    table_name=i["object_name"],
                ).first()
                if tab:
                    return -rule.weight, [
                        i["statement_id"],
                        i["plan_hash_value"],
                        i["object_name"],
                        i["the_id"],
                        i["cost"],
                        i["count"]
                    ]
    return None, []


code_hole.append(code)
