import re

from mongoengine import Q

from utils.mongo_utils import *
from models.mongo import ObjTabInfo, OracleTicketSQLPlan


def code(rule, **kwargs):
    '''
    db.@sql@.find({
        $or: [
                {OPERATION: /NESTED LOOP/},
                {OPERATION: /FILTER/}
             ],
        USERNAME: '@username@', record_id: '@record_id@'
    }).forEach(function(x){db.@tmp@.save({
        SQL_ID: x.SQL_ID, PLAN_HASH_VALUE: x.PLAN_HASH_VALUE, PARENT_ID: x.ID,
        USERNAME: x.USERNAME, record_id:x.record_id
    });});

    db.@tmp@.find().forEach(function(x){db.sqlplan.find({
        SQL_ID: x.SQL_ID, PLAN_HASH_VALUE: x.PLAN_HASH_VALUE, PARENT_ID: x.PARENT_ID,
        USERNAME: x.USERNAME, record_id: x.record_id
    }).forEach(function(y){db.@tmp1@.save({
        SQL_ID: y.SQL_ID, PLAN_HASH_VALUE: y.PLAN_HASH_VALUE, OBJECT_NAME: y.OBJECT_NAME,
        ID: y.ID, PARENT_ID: y.PARENT_ID, OPERATION: y.OPERATION, OPTIONS: y.OPTIONS,
        USERNAME: y.USERNAME, record_id: y.record_id})});});

    db.@tmp@.drop();
    db.@tmp1@.aggregate([
        {$group:{_id:{PARENT_ID:\"$PARENT_ID\",SQL_ID:\"$SQL_ID\",
        PLAN_HASH_VALUE:\"$PLAN_HASH_VALUE\"},
        MAXID: {$max:\"$ID\"}}}]).forEach(function(z){db.sqlplan.find({
            SQL_ID:z._id.SQL_ID,
            PLAN_HASH_VALUE:z._id.PLAN_HASH_VALUE,
            $and:[
                {ID:z.MAXID},{ID:{$ne:2}}
            ],
            \"USERNAME\":\"@username@\",
            record_id: '@record_id@',
            OPERATION:\"TABLE ACCESS\",
            OPTIONS:\"FULL\"
            }).forEach(function(y){if(db.obj_tab_info.findOne({
                OWNER: y.OBJECT_OWNER, IPADDR: '@ip_addr@', SID: '@sid@',
                TABLE_NAME: y.OBJECT_NAME,
                $or: [{\"NUM_ROWS\":{$gt:@table_row_num@}},{\"PHY_SIZE(MB)\":{$gt:@table_phy_size@}}]}))
                db.@tmp@.save({
                    \"SQL_ID\":y.SQL_ID,\"PLAN_HASH_VALUE\":y.PLAN_HASH_VALUE,\"OBJECT_NAME\":y.OBJECT_NAME,
                    \"ID\":y.ID,\"COST\":y.COST,\"COUNT\":\"\"})});})",
    '''

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
                    return -rule.weight, [
                        paa.statement_id,
                        paa.plan_id,
                        paa.object_name,
                        paa.the_id,
                        paa.cost
                    ]

    return None, []


code_hole.append(code)
