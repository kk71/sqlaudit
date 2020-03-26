import re

from models.mongo import OracleTicketSQLPlan


def code(rule, entries, **kwargs):
    """"db.@sql@.group( {
    key:{\"USERNAME\":1,\"ETL_DATE\":1,\"SQL_ID\":1,\"PLAN_HASH_VALUE\":1},
     cond:{\"USERNAME\":\"@username@\",\"@etl_date_key@\":\"@etl_date@\",
     $or: [{\"OPERATION\":/NESTED LOOP/},{\"OPERATION\":/FILTER/}]},
     reduce:function(curr,result){ result.count++; }, initial:{count:0} } ).

     forEach(function(x){db.@tmp1@.save({\"SQL_ID\":x.SQL_ID,
     \"PLAN_HASH_VALUE\":x.PLAN_HASH_VALUE,\"COUNT\":x.count})});
     db.@tmp1@.find({\"COUNT\":{$gte:\"@loop_num@\"}}).
     forEach(function(y){db.@tmp@.save({\"SQL_ID\":y.SQL_ID,
     \"PLAN_HASH_VALUE\":y.PLAN_HASH_VALUE,\"ID\":y.ID,\"COUNT\":x.COUNT,
     \"COST\":\"\",\"OBJECT_NAME\":\"\"})})",
    """
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


