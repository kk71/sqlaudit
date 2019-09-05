# Author: kk.Fang(fkfkbill@gmail.com)

import re
import random
from contextlib import contextmanager


def get_random_collection_name(prefix):
    """产生一个随机的collection名称，用于aggregation"""
    seed = list("1234567890qazwsxedcrfvtgbyhnujmikolp")
    random.shuffle(seed)
    rad = "".join(seed[:9])
    return f"{prefix}-{rad}"


@contextmanager
def make_temp_collection(mongo_client, collection_prefix):
    """更安全的使用一个临时的collection"""
    collection_name = None
    try:
        collection_name = get_random_collection_name(collection_prefix)
        yield mongo_client.get_collection(collection_name)
        mongo_client.drop(collection_name)
    except Exception as e:
        print(f"exception occurs when operating temporary collection {collection_name}, going to drop it.")
        mongo_client.drop(collection_name)
        raise e


def LOOP_IN_TAB_FULL_SCAN(mongo_client, sql, username, etl_date_key, etl_date, **kwargs):
    '''
    db.@sql@.find({$or: [{"OPERATION":/NESTED LOOP/},{"OPERATION":/FILTER/}],"USERNAME":"@username@","@etl_date_key@":"@etl_date@"}).forEach(function(x){db.@tmp@.save({SQL_ID:x.SQL_ID,PLAN_HASH_VALUE:x.PLAN_HASH_VALUE,PARENT_ID:x.ID,USERNAME:x.USERNAME,ETL_DATE:x.ETL_DATE});});
    db.@tmp@.find().forEach(  function(x){  db.sqlplan.find({SQL_ID:x.SQL_ID,PLAN_HASH_VALUE:x.PLAN_HASH_VALUE,PARENT_ID:x.PARENT_ID,USERNAME:x.USERNAME,ETL_DATE:x.ETL_DATE}).forEach(function(y){db.@tmp1@.save({SQL_ID:y.SQL_ID,PLAN_HASH_VALUE:y.PLAN_HASH_VALUE,OBJECT_NAME:y.OBJECT_NAME,ID:y.ID,PARENT_ID:y.PARENT_ID,OPERATION:y.OPERATION,OPTIONS:y.OPTIONS,USERNAME:y.USERNAME,ETL_DATE:y.ETL_DATE})});});
    db.@tmp@.drop();
    db.@tmp1@.aggregate([{$group:{_id:{PARENT_ID:"$PARENT_ID",SQL_ID:"$SQL_ID",PLAN_HASH_VALUE:"$PLAN_HASH_VALUE"},MAXID: {$max:"$ID"}}}]).forEach(function(z){db.sqlplan.find({SQL_ID:z._id.SQL_ID,PLAN_HASH_VALUE:z._id.PLAN_HASH_VALUE,$and:[{ID:z.MAXID},{ID:{$ne:2}}],"USERNAME":"@username@","@etl_date_key@":"@etl_date@",OPERATION:"TABLE ACCESS",OPTIONS:"FULL"}).forEach(function(y){if(db.obj_tab_info.findOne({"OWNER":y.OBJECT_OWNER,"ETL_DATE":y.ETL_DATE,"TABLE_NAME":y.OBJECT_NAME,$or: [{"NUM_ROWS":{$gt:@table_row_num@}},{"PHY_SIZE(MB)":{$gt:@table_phy_size@}}]}))db.@tmp@.save({"SQL_ID":y.SQL_ID,"PLAN_HASH_VALUE":y.PLAN_HASH_VALUE,"OBJECT_NAME":y.OBJECT_NAME,"ID":y.ID,"COST":y.COST,"COUNT":""})});})
    '''
    sqlplan_collection = mongo_client.get_collection("sqlplan")
    obj_tab_info_collection = mongo_client.get_collection("obj_tab_info")

    found_items = sql.find({
        "$or": [
            {"OPERATION": re.compile("NESTED LOOP")},
            {"OPERATION": re.compile("FILTER")}
        ],
        "USERNAME": username,
        etl_date_key: etl_date
    })

    with make_temp_collection(mongo_client, "LOOP_IN_TAB_FULL_SCAN") as tmp1:
        for x in found_items:
            condition_to_find_in_sqlplan = {
                "SQL_ID": x["SQL_ID"],
                "PLAN_HASH_VALUE": x["PLAN_HASH_VALUE"],
                "PARENT_ID": x["ID"],
                "USERNAME": x["USERNAME"],
                "ETL_DATE": x["ETL_DATE"]
            }
            found_in_sqlplan = sqlplan_collection.find(condition_to_find_in_sqlplan)
            for y in found_in_sqlplan:
                tmp1.insert_one({
                    "SQL_ID": y["SQL_ID"],
                    "PLAN_HASH_VALUE": y["PLAN_HASH_VALUE"],
                    "OBJECT_NAME": y["OBJECT_NAME"],
                    "ID": y["ID"],
                    "PARENT_ID": y["PARENT_ID"],
                    "OPERATION": y["OPERATION"],
                    "OPTIONS": y["OPTIONS"],
                    "USERNAME": y["USERNAME"],
                    "ETL_DATE": y["ETL_DATE"]
                })

        aggregation_result = tmp1.aggregate([
            {"$group": {
                "_id": {
                    "PARENT_ID": "$PARENT_ID",
                    "SQL_ID": "$SQL_ID",
                    "PLAN_HASH_VALUE": "$PLAN_HASH_VALUE"
                },
                "MAXID": {"$max": "$ID"}
            }}
        ])
        for z in aggregation_result:
            collection_sqlplan_find_result = sqlplan_collection.find({
                "SQL_ID": z["_id"]["SQL_ID"],
                "PLAN_HASH_VALUE": z["_id"]["PLAN_HASH_VALUE"],
                "$and":[
                    {"ID": z["MAXID"]},
                    {"ID": {"$ne": 2}}
                ],
                "USERNAME": username,
                etl_date_key: etl_date,
                "OPERATION": "TABLE ACCESS",
                "OPTIONS": "FULL"})
            for y in collection_sqlplan_find_result:
                find_one_rst = obj_tab_info_collection.find_one({
                    "OWNER": y["OBJECT_OWNER"],
                    "ETL_DATE": y["ETL_DATE"],
                    "TABLE_NAME": y["OBJECT_NAME"],
                    "$or":[
                        {"NUM_ROWS": {"$gt": kwargs["table_row_num"]}},
                        {"PHY_SIZE(MB)": {"$gt": kwargs["table_phy_size"]}}
                    ]
                })
                if find_one_rst:
                    yield {
                        "SQL_ID": y["SQL_ID"],
                        "PLAN_HASH_VALUE": y["PLAN_HASH_VALUE"],
                        "OBJECT_NAME": y["OBJECT_NAME"],
                        "ID": y["ID"],
                        "COST": y["COST"],
                        "COUNT": ""
                    }
