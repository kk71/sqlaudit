# Author: kk.Fang(fkfkbill@gmail.com)

import re
import random
from contextlib import contextmanager

from utils.perf_utils import timing


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


# TODO
@timing()
def SQL_TAB_REL_NUM(mongo_client, sql, username, etl_date_key, etl_date, key, cond, reduce, **kwargs):
    """db.@sql@.group( { key:{\"USERNAME\":1,\"ETL_DATE\":1,\"SQL_ID\":1,\"PLAN_HASH_VALUE\":1,
    \"OBJECT_TYPE\":1}, cond:{\"USERNAME\":\"@username@\",\"@etl_date_key@\":\"@etl_date@\",
    \"OBJECT_TYPE\":\"TABLE\"}, reduce:function(curr,result){ result.count++; }, initial:{count:0} } )
    .forEach(function(x){db.@tmp1@.save({\"SQL_ID\":x.SQL_ID,\"PLAN_HASH_VALUE\":x.PLAN_HASH_VALUE,
    \"ID\":x.ID,\"COUNT\":x.count})});
    db.@tmp1@.find({\"COUNT\":{$gte:@tab_num@}}).
    forEach(function(y){db.@tmp@.save({\"SQL_ID\":y.SQL_ID,\"PLAN_HASH_VALUE\":y.PLAN_HASH_VALUE,
    \"ID\":y.ID,\"COUNT\":y.COUNT,\"COST\":\"\",\"OBJECT_NAME\":\"\"})})"""
    # sql_collection=mongo_client.get_collection(sql)
    #
    # found_items=sql_collection.group({"key":{"USERNAME":1,
    #                                        "ETL_DATE":1,
    #                                        "SQL_ID":1,
    #                                        "PLAN_HASH_VALUE":1,
    #                                        "OBJECT_TYPE":1},
    #                                   "cond":{"USERNAME":username,
    #                                         etl_date_key:etl_date,
    #                                         "OBJECT_TYPE":"TABLE"},
    #                                   "reduce":{}})
    sql_collection = mongo_client.get_collection(sql)
    sql_collection.group({
        "key": {
            "USERNAME": 1,
            "ETL_DATE": 1,
            "SQL_ID": 1,
            "PLAN_HASH_VALUE": 1,
            "OBJECT_TYPE": 1
        },
        "cond": {

        }
    })


@timing()
def SQL_VIEW_SCAN(mongo_client, sql, username, etl_date_key, etl_date, **kwargs):
    """"db.@sql@.find({\"OBJECT_TYPE\":\"VIEW\",\"USERNAME\":\"@username@\",
    \"@etl_date_key@\":\"@etl_date@\",\"OBJECT_OWNER\":{$ne:null},\"OBJECT_NAME\":/^index$_join$/}).
    forEach(function(x)
    {db.@tmp@.save({\"SQL_ID\":x.SQL_ID,\"PLAN_HASH_VALUE\":x.PLAN_HASH_VALUE,
    \"OBJECT_NAME\":x.OBJECT_NAME,\"ID\":x.ID,\"COST\":x.COST,\"COUNT\":\"\"})})"""
    sql_collection = mongo_client.get_collection(sql)

    fount_items = sql_collection.find({
        "OBJECT_TYPE": "VIEW",
        "USERNAME": username,
        etl_date_key: etl_date,
        "OBJECT_OWNER": {"$ne": None},
        "OBJECT_NAME": re.compile(r'^index$_join$')
    })
    for x in fount_items:
        yield {
            "SQL_ID": x["SQL_ID"],
            "PLAN_HASH_VALUE": x["PLAN_HASH_VALUE"],
            "OBJECT_NAME": x["OBJECT_NAME"],
            "ID": x["ID"],
            "COST": x["COST"],
            "COUNT": ""
        }


def SQL_LOOP_NUM(mongo_client, sql, username, etl_date_key, etl_date, **kwargs):
    """db.@sql@.group( { key:{\"USERNAME\":1,\"ETL_DATE\":1,\"SQL_ID\":1,\"PLAN_HASH_VALUE\":1},
     cond:{\"USERNAME\":\"@username@\",\"@etl_date_key@\":\"@etl_date@\",
     $or: [{\"OPERATION\":/NESTED LOOP/},{\"OPERATION\":/FILTER/}]},
     reduce:function(curr,result){ result.count++; }, initial:{count:0} } ).
     forEach(function(x)
     {db.@tmp1@.save({\"SQL_ID\":x.SQL_ID,\"PLAN_HASH_VALUE\":x.PLAN_HASH_VALUE,\"COUNT\":x.count})});
     db.@tmp1@.find({\"COUNT\":{$gte:\"@loop_num@\"}}).
     forEach(function(y){db.@tmp@.save({\"SQL_ID\":y.SQL_ID,\"PLAN_HASH_VALUE\":y.PLAN_HASH_VALUE,
     \"ID\":y.ID,\"COUNT\":x.COUNT,\"COST\":\"\",\"OBJECT_NAME\":\"\"})})
    """
    pass


@timing()
def LOOP_IN_TAB_FULL_SCAN(mongo_client, sql, username, etl_date_key, etl_date, **kwargs):
    '''
    db.@sql@.find({$or: [{"OPERATION":/NESTED LOOP/},{"OPERATION":/FILTER/}],"USERNAME":"@username@",
    "@etl_date_key@":"@etl_date@"}).
    forEach(function(x){db.@tmp@.save({SQL_ID:x.SQL_ID,PLAN_HASH_VALUE:x.PLAN_HASH_VALUE,PARENT_ID:x.ID,
    USERNAME:x.USERNAME,ETL_DATE:x.ETL_DATE});});
    db.@tmp@.find().
    forEach(  function(x){  db.sqlplan.find({SQL_ID:x.SQL_ID,PLAN_HASH_VALUE:x.PLAN_HASH_VALUE,
    PARENT_ID:x.PARENT_ID,USERNAME:x.USERNAME,ETL_DATE:x.ETL_DATE}).
    forEach(function(y){db.@tmp1@.save({SQL_ID:y.SQL_ID,PLAN_HASH_VALUE:y.PLAN_HASH_VALUE,
    OBJECT_NAME:y.OBJECT_NAME,ID:y.ID,PARENT_ID:y.PARENT_ID,OPERATION:y.OPERATION,OPTIONS:y.OPTIONS,
    USERNAME:y.USERNAME,ETL_DATE:y.ETL_DATE})});});
    db.@tmp@.drop();
    db.@tmp1@.aggregate([{$group:
    {_id:{PARENT_ID:"$PARENT_ID",SQL_ID:"$SQL_ID",PLAN_HASH_VALUE:"$PLAN_HASH_VALUE"},
    MAXID: {$max:"$ID"}}}]).
    forEach(function(z){db.sqlplan.find({SQL_ID:z._id.SQL_ID,PLAN_HASH_VALUE:z._id.PLAN_HASH_VALUE,
    $and:[{ID:z.MAXID},{ID:{$ne:2}}],"USERNAME":"@username@","@etl_date_key@":"@etl_date@",
    OPERATION:"TABLE ACCESS",OPTIONS:"FULL"}).
    forEach(function(y){if(db.obj_tab_info.findOne({"OWNER":y.OBJECT_OWNER,"ETL_DATE":y.ETL_DATE,
    "TABLE_NAME":y.OBJECT_NAME,$or: [{"NUM_ROWS":{$gt:@table_row_num@}},
    {"PHY_SIZE(MB)":{$gt:@table_phy_size@}}]}))
    db.@tmp@.save({"SQL_ID":y.SQL_ID,"PLAN_HASH_VALUE":y.PLAN_HASH_VALUE,"OBJECT_NAME":y.OBJECT_NAME,
    "ID":y.ID,"COST":y.COST,"COUNT":""})});})
    '''
    sql_collection = mongo_client.get_collection(sql)
    sqlplan_collection = mongo_client.get_collection("sqlplan")
    obj_tab_info_collection = mongo_client.get_collection("obj_tab_info")
    found_items = sql_collection.find({
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
                "$and": [
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
                    "$or": [
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


@timing()
def SQL_PARTITION_RANGE_ALL(mongo_client, sql, username, etl_date_key, etl_date, **kwargs):
    """"db.@sql@.find({\"OPERATION\":\"PARTITION RANGE\",\"OPTIONS\":\"ALL\",\"USERNAME\":\"@username@\",
    \"@etl_date_key@\":\"@etl_date@\"}).
    forEach(function(x){db.@sql@.find({\"SQL_ID\":x.SQL_ID,\"PLAN_HASH_VALUE\":x.PLAN_HASH_VALUE,
    \"ID\":{$eq:x.ID+1}}).
    forEach(function(y)
    {db.@tmp@.save({\"SQL_ID\":y.SQL_ID,\"PLAN_HASH_VALUE\":y.PLAN_HASH_VALUE,\"OBJECT_NAME\":y.OBJECT_NAME,
    \"ID\":y.ID,\"COST\":x.COST,\"COUNT\":\"\"})});})"""
    sql_collection = mongo_client.get_collection(sql)

    found_items = sql_collection.find({
        "OPERATION": "PARTITION RANGE",
        "OPTIONS": "ALL",
        "USERNAME": username,
        etl_date_key: etl_date
    })

    for x in found_items:
        condition_find_sql = {
            "SQL_ID": x["SQL_ID"],
            "PLAN_HASH_VALUE": x["PLAN_HASH_VALUE"],
            "ID": x["ID"] + 1
        }
        condition_find_sql = sql_collection.find(condition_find_sql)

        for y in condition_find_sql:
            yield {
                "SQL_ID": y["SQL_ID"],
                "PLAN_HASH_VALUE": y["PLAN_HASH_VALUE"],
                "OBJECT_NAME": y["OBJECT_NAME"],
                "ID": y["ID"],
                "COST": x["COST"],
                "COUNT": ""
            }


@timing()
def SQL_INDEX_FAST_FULL_SCAN(mongo_client, sql, username, etl_date_key, etl_date, ind_phy_size, **kwargs):
    """db.@sql@.find({\"OPERATION\":\"INDEX\",\"OPTIONS\":\"FAST FULL SCAN\",\"USERNAME\":\"@username@\",
    \"@etl_date_key@\":\"@etl_date@\"}).

    forEach(function(x)
    {if(db.obj_ind_info.findOne({\"INDEX_NAME\":x.OBJECT_NAME,\"PHY_SIZE(MB)\":{$gt:@ind_phy_size@}}))
    db.@tmp@.save({\"SQL_ID\":x.SQL_ID,\"PLAN_HASH_VALUE\":x.PLAN_HASH_VALUE,\"OBJECT_NAME\":x.OBJECT_NAME,
    \"ID\":x.ID,\"COST\":x.COST,\"COUNT\":\"\"});})"""
    sql_collection = mongo_client.get_collection(sql)
    obj_ind_info_collection = mongo_client.get_collection("obj_ind_info")

    found_items = sql_collection.find(
        {"OPERATION": "INDEX",
         "OPTIONS": "FAST FULL SCAN",
         "USERNAME": username,
         etl_date_key: etl_date})

    for x in found_items:
        first_obj_ind_info = obj_ind_info_collection.find_one({
            "INDEX_NAME": x["OBJECT_NAME"],
            "PHY_SIZE(MB)": {"$gt": ind_phy_size}})
        if first_obj_ind_info:
            yield {
                "SQL_ID": x["SQL_ID"],
                "PLAN_HASH_VALUE": x["PLAN_HASH_VALUE"],
                "OBJECT_NAME": x["OBJECT_NAME"],
                "ID": x["ID"],
                "COST": x["COST"],
                "COUNT": ""
            }


@timing()
def SQL_PARTITION_RANGE_ITERATOR(mongo_client, sql, username, etl_date_key, etl_date, **kwargs):
    """db.@sql@.find({\"OPERATION\":\"PARTITION RANGE\",\"OPTIONS\":\"ITERATOR\",
    \"USERNAME\":\"@username@\",\"@etl_date_key@\":\"@etl_date@\"}).
    forEach(function(x)
    {db.@sql@.find({\"SQL_ID\":x.SQL_ID,\"PLAN_HASH_VALUE\":x.PLAN_HASH_VALUE,\"ID\":{$eq:x.ID+1},
    \"USERNAME\":\"@username@\",\"@etl_date_key@\":\"@etl_date@\"}).
    forEach(function(y)
    {db.@tmp@.save({\"SQL_ID\":y.SQL_ID,\"PLAN_HASH_VALUE\":y.PLAN_HASH_VALUE,
    \"OBJECT_NAME\":y.OBJECT_NAME,\"ID\":y.ID,\"COST\":x.COST,\"COUNT\":\"\"})});})"""
    sql_collection = mongo_client.get_collection(sql)
    found_items = sql_collection.find({
        "OPERATION": "PARTITION RANGE",
        "OPTIONS": "ITERATOR",
        "USERNAME": username,
        etl_date_key: etl_date
    })
    for x in found_items:
        condition_find_sql = {
            "SQL_ID": x["SQL_ID"],
            "PLAN_HASH_VALUE": x["PLAN_HASH_VALUE"],
            "ID": x["ID"] + 1,
            "USERNAME": username,
            etl_date_key: etl_date
        }
        condition_find_sql = sql_collection.find(condition_find_sql)
        for y in condition_find_sql:
            yield {
                "SQL_ID": y["SQL_ID"],
                "PLAN_HASH_VALUE": y["PLAN_HASH_VALUE"],
                "OBJECT_NAME": y["OBJECT_NAME"],
                "ID": y["ID"],
                "COST": x["COST"],
                "COUNT": ""
            }


@timing()
def SQL_TABLE_FULL_SCAN(mongo_client, sql, username, etl_date_key, etl_date, table_row_num, table_phy_size, **kwargs):
    """db. @ sql @.find({\"OPERATION\":\"TABLE ACCESS\",\"OPTIONS\":\"FULL\",\"USERNAME\":\"@username@\",
    \"@etl_date_key@\":\"@etl_date@\"}).
    forEach(function(x){if(db.obj_tab_info.findOne({\"TABLE_NAME\":x.OBJECT_NAME,
    $or: [{\"NUM_ROWS\":{$gt:@table_row_num@}},{\"PHY_SIZE(MB)\":{$gt:@table_phy_size@}}]}))
    db.@tmp@.save({\"SQL_ID\":x.SQL_ID,\"PLAN_HASH_VALUE\":x.PLAN_HASH_VALUE,
    \"OBJECT_NAME\":x.OBJECT_NAME,\"ID\":x.ID,\"COST\":x.COST,\"COUNT\":\"\"});})"""
    sql_collection = mongo_client.get_collection(sql)
    obj_tab_info_collection = mongo_client.get_collection("obj_tab_info")
    found_items = sql_collection.find({
        "OPERATION": "TABLE ACCESS",
        "OPTIONS": "FULL",
        "USERNAME": username,
        etl_date_key: etl_date
    })
    for x in found_items:
        first_obj_tab_info = obj_tab_info_collection.find_one({
            "TABLE_NAME": x["OBJECT_NAME"],
            "$or": [
                {"NUM_ROWS": {"$gt": table_row_num}},
                {"PHY_SIZE(MB)": {"$gt": table_phy_size}}
            ]
        })
        if first_obj_tab_info:
            yield {
                "SQL_ID": x["SQL_ID"],
                "PLAN_HASH_VALUE": x["PLAN_HASH_VALUE"],
                "OBJECT_NAME": x["OBJECT_NAME"],
                "ID": x["ID"],
                "COST": x["COST"],
                "COUNT": ""
            }


@timing()
def SQL_MERGE_JOIN_CARTESIAN(mongo_client, sql, username, etl_date_key, etl_date, **kwargs):
    """db.@sql@.find({\"OPERATION\":\"MERGE JOIN\",\"OPTIONS\":\"CARTESIAN\",\"USERNAME\":\"@username@\",
    \"@etl_date_key@\":\"@etl_date@\"}).
    forEach(function(x)
    {db.@tmp@.save({\"SQL_ID\":x.SQL_ID,\"PLAN_HASH_VALUE\":x.PLAN_HASH_VALUE,\"OBJECT_NAME\":x.OBJECT_NAME,
    \"ID\":x.ID,\"COST\":x.COST,\"COUNT\":\"\"});})"""
    sql_collection = mongo_client.get_collection(sql)
    found_items = sql_collection.find({
        "OPERATION": "MERGE JOIN",
        "OPTIONS": "CARTESIAN",
        "USERNAME": username,
        etl_date_key: etl_date
    })
    for x in found_items:
        yield {
            "SQL_ID": x["SQL_ID"],
            "PLAN_HASH_VALUE": x["PLAN_HASH_VALUE"],
            "OBJECT_NAME": x["OBJECT_NAME"],
            "ID": x["ID"],
            "COST": x["COST"],
            "COUNT": ""
        }


@timing()
def SQL_INDEX_SKIP_SCAN(mongo_client, sql, username, etl_date_key, etl_date, **kwargs):
    """db.@sql@.find({\"OPERATION\":\"INDEX\",\"OPTIONS\":\"SKIP SCAN\",\"USERNAME\":\"@username@\",
    \"@etl_date_key@\":\"@etl_date@\"}).
    forEach(function(x)
    {db.@tmp@.save({\"SQL_ID\":x.SQL_ID,\"PLAN_HASH_VALUE\":x.PLAN_HASH_VALUE,
    \"OBJECT_NAME\":x.OBJECT_NAME,\"ID\":x.ID,\"COST\":x.COST,\"COUNT\":\"\"});})"""
    sql_collection = mongo_client.get_collection(sql)
    found_items = sql_collection.find({
        "OPERATION": "INDEX",
        "OPTIONS": "SKIP SCAN",
        "USERNAME": username,
        etl_date_key: etl_date
    })
    for x in found_items:
        yield {
            "SQL_ID": x["SQL_ID"],
            "PLAN_HASH_VALUE": x["PLAN_HASH_VALUE"],
            "OBJECT_NAME": x["OBJECT_NAME"],
            "ID": x["ID"],
            "COST": x["COST"],
            "COUNT": ""
        }


@timing()
def SQL_INDEX_FULL_SCAN(mongo_client, sql, username, etl_date_key, etl_date, ind_phy_size, **kwargs):
    """db.@sql@.find({\"OPERATION\":\"INDEX\",\"OPTIONS\":\"FULL SCAN\",\"USERNAME\":\"@username@\",
    \"@etl_date_key@\":\"@etl_date@\"}).
    forEach(function(x)
    {if(db.obj_ind_info.findOne({\"INDEX_NAME\":x.OBJECT_NAME,\"PHY_SIZE(MB)\":{$gt:@ind_phy_size@}}))
    db.@tmp@.save({\"SQL_ID\":x.SQL_ID,\"PLAN_HASH_VALUE\":x.PLAN_HASH_VALUE,\"OBJECT_NAME\":x.OBJECT_NAME,
    \"ID\":x.ID,\"COST\":x.COST,\"COUNT\":\"\"});})"""

    sql_collection = mongo_client.get_collection(sql)
    obj_ind_info_collection = mongo_client.get_collection("obj_ind_info")
    found_items = sql_collection.find({
        "OPERATION": "INDEX",
        "OPTIONS": "FULL SCAN",
        "USERNAME": username,
        etl_date_key: etl_date
    })
    for x in found_items:
        first_obj_ind_info = obj_ind_info_collection.find_one({
            "INDEX_NAME": x["OBJECT_NAME"],
            "PHY_SIZE(MB)": {"$gt": ind_phy_size}
        })
        if first_obj_ind_info:
            yield {
                "SQL_ID": x["SQL_ID"],
                "PLAN_HASH_VALUE": x["PLAN_HASH_VALUE"],
                "OBJECT_NAME": x["OBJECT_NAME"],
                "ID": x["ID"],
                "COST": x["COST"],
                "COUNT": ""
            }


@timing()
def SQL_PARTITION_RANGE_INLIST_OR(mongo_client, sql, username, etl_date_key, etl_date, **kwargs):
    """db.@sql@.find({\"OPERATION\":\"PARTITION RANGE\",
    $or: [{\"OPTIONS\":\"INLIST\"},{\"OPTIONS\":\"OR\"}],
    \"USERNAME\":\"@username@\",\"@etl_date_key@\":\"@etl_date@\"}).
    forEach(function(x)
    {db.@sql@.find({\"SQL_ID\":x.SQL_ID,\"PLAN_HASH_VALUE\":x.PLAN_HASH_VALUE,\"ID\":{$eq:x.ID+1}}).
    forEach(function(y)
    {db.@tmp@.save({\"SQL_ID\":y.SQL_ID,\"PLAN_HASH_VALUE\":y.PLAN_HASH_VALUE,
    \"OBJECT_NAME\":y.OBJECT_NAME,\"ID\":y.ID,\"COST\":x.COST,\"COUNT\":\"\"})});})"""
    sql_collection = mongo_client.get_collection(sql)
    found_items = sql_collection.find({
        "OPERATION": "PARTITION RANGE",
        "$or": [{"OPTIONS": "INLIST"}, {"OPTIONS": "OR"}],
        "USERNAME": username,
        etl_date_key: etl_date
    })
    for x in found_items:
        condition_find_sql = sql_collection.find({
            "SQL_ID": x["SQL_ID"],
            "PLAN_HASH_VALUE": x["PLAN_HASH_VALUE"],
            "ID": {"$eq": x["ID"] + 1}
        })
        for y in condition_find_sql:
            yield {
                "SQL_ID": y["SQL_ID"],
                "PLAN_HASH_VALUE": y["PLAN_HASH_VALUE"],
                "OBJECT_NAME": y["OBJECT_NAME"],
                "ID": y["ID"],
                "COST": x["COST"],
                "COUNT": ""
            }


@timing()
def SQL_PARALLEL_FETCH(mongo_client, sql, username, etl_date_key, etl_date, **kwargs):
    """db.@sql@.find({\"OPERATION\":\"PX COORDINATOR\",\"USERNAME\":\"@username@\",
    \"@etl_date_key@\":\"@etl_date@\"}).
    forEach(function(x)
    {db.@tmp@.save({\"SQL_ID\":x.SQL_ID,\"PLAN_HASH_VALUE\":x.PLAN_HASH_VALUE,
    \"OBJECT_NAME\":x.OBJECT_NAME,\"ID\":x.ID,\"COST\":x.COST,\"COUNT\":\"\"});})"""
    sql_collection = mongo_client.get_collection(sql)
    found_items = sql_collection.find({
        "OPERATION": "PX COORDINATOR",
        "USERNAME": username,
        etl_date_key: etl_date
    })

    for x in found_items:
        yield {
            "SQL_ID": x["SQL_ID"],
            "PLAN_HASH_VALUE": x["PLAN_HASH_VALUE"],
            "OBJECT_NAME": x["OBJECT_NAME"],
            "ID": x["ID"],
            "COST": x["COST"],
            "COUNT": ""
        }


@timing()
def SQL_BUFFER_GETS(mongo_client, sql, username, etl_date_key, etl_date, buffer_gets, **kwargs):
    """db.@sql@.find({\"PER_BUFFER_GETS\":{$gte:@buffer_gets@},\"USERNAME\":\"@username@\",
    \"@etl_date_key@\":\"@etl_date@\"}).
    forEach(function(x)
    {db.@tmp@.save({\"SQL_ID\":x.SQL_ID,\"PLAN_HASH_VALUE\":x.PLAN_HASH_VALUE})})"""
    sql_collection = mongo_client.get_collection(sql)

    found_items = sql_collection.find({
        "PER_BUFFER_GETS": {"$gte": buffer_gets},
        "USERNAME": username,
        etl_date_key: etl_date
    })
    for x in found_items:
        yield {
            "SQL_ID": x["SQL_ID"],
            "PLAN_HASH_VALUE": x["PLAN_HASH_VALUE"]
        }


@timing()
def SQL_SUB_CURSOR_COUNT(mongo_client, sql, username, etl_date_key, etl_date, cursor_num, **kwargs):
    """db.@sql@.find({\"VERSION_COUNT\":{$gte:@cursor_num@},\"USERNAME\":\"@username@\",
    \"@etl_date_key@\":\"@etl_date@\"}).
    forEach(function(x)
    {db.@tmp@.save({\"SQL_ID\":x.SQL_ID,\"PLAN_HASH_VALUE\":x.PLAN_HASH_VALUE})})"""
    sql_collection = mongo_client.get_collection(sql)
    found_items = sql_collection.find({
        "VERSION_COUNT": {"$gte": cursor_num},
        "USERNAME": username,
        etl_date_key: etl_date
    })
    for x in found_items:
        yield {
            "SQL_ID": x["SQL_ID"],
            "PLAN_HASH_VALUE": x["PLAN_HASH_VALUE"]
        }


@timing()
def SQL_CPU_TIME(mongo_client, sql, username, etl_date_key, etl_date, cpu_time, **kwargs):
    """db.@sql@.find({\"PER_CPU_TIME\":{$gte:@cpu_time@},\"USERNAME\":\"@username@\",
    \"@etl_date_key@\":\"@etl_date@\"}).
    forEach(function(x)
    {db.@tmp@.save({\"SQL_ID\":x.SQL_ID,\"PLAN_HASH_VALUE\":x.PLAN_HASH_VALUE})})"""
    sql_collection = mongo_client.get_collection(sql)
    statement = {
        "PER_CPU_TIME": {"$gte": int(cpu_time)},
        "USERNAME": username,
        etl_date_key: etl_date
    }
    print(statement)
    found_items = sql_collection.find(statement)
    for x in found_items:
        yield {
            "SQL_ID": x["SQL_ID"],
            "PLAN_HASH_VALUE": x["PLAN_HASH_VALUE"]
        }


@timing()
def SQL_EXECUTIONS(mongo_client, sql, username, etl_date_key, etl_date, sql_count_num, **kwargs):
    """db. @ sql @.find({\"EXECUTIONS\":{$gte:@sql_count_num@},\"USERNAME\":\"@username@\",
    \"@etl_date_key@\":\"@etl_date@\"}).
    forEach(function(x)
    {db.@tmp@.save({\"SQL_ID\":x.SQL_ID,\"PLAN_HASH_VALUE\":x.PLAN_HASH_VALUE})})"""
    sql_collection = mongo_client.get_collection(sql)
    found_items = sql_collection.find({
        "EXECUTIONS": {"$gte": sql_count_num},
        "USERNMAE": username,
        etl_date_key: etl_date
    })
    for x in found_items:
        yield {
            "SQL_ID": x["SQL_ID"],
            "PLAN_HASH_VALUE": x["PLAN_HASH_VALUE"]
        }


@timing()
def SQL_ELAPSED_TIME(mongo_client, sql, username, etl_date_key, etl_date, elapsed_time, **kwargs):
    """db.@sql@.find({\"PER_ELAPSED_TIME\":{$gte:@elapsed_time@},\"USERNAME\":\"@username@\",
    \"@etl_date_key@\":\"@etl_date@\"}).
    forEach(function(x)
    {db.@tmp@.save({\"SQL_ID\":x.SQL_ID,\"PLAN_HASH_VALUE\":x.PLAN_HASH_VALUE})})"""
    sql_collection = mongo_client.get_collection(sql)
    found_items = sql_collection.find({
        "PER_ELAPSED_TIME": {"$gte": elapsed_time},
        "USERNAME": username,
        etl_date_key: etl_date
    })
    for x in found_items:
        yield {
            "SQL_ID": x["SQL_ID"],
            "PLAN_HASH_VALUE": x["PLAN_HASH_VALUE"]
        }


@timing()
def SQL_DISK_READS(mongo_client, sql, username, etl_date_key, etl_date, disk_reads, **kwargs):
    """db.@sql@.find({\"PER_DISK_READS\":{$gte:@disk_reads@},\"USERNAME\":\"@username@\",
    \"@etl_date_key@\":\"@etl_date@\"}).
    forEach(function(x)
    {db.@tmp@.save({\"SQL_ID\":x.SQL_ID,\"PLAN_HASH_VALUE\":x.PLAN_HASH_VALUE})})"""
    sql_collection = mongo_client.get_collection(sql)
    found_items = sql_collection.find({
        "PER_DISK_READS": {"$gte": disk_reads},
        "USERNAME": username,
        etl_date_key: etl_date
    })
    for x in found_items:
        yield {
            "SQL_ID": x["SQL_ID"],
            "PLAN_HASH_VALUE": x["PLAN_HASH_VALUE"]
        }


@timing()
def SQL_DIRECT_WRITES(mongo_client, sql, username, etl_date_key, etl_date, direct_writes, **kwargs):
    """db.@sql@.find({\"PER_DIRECT_WRITES\":{$gte:@direct_writes@},\"USERNAME\":\"@username@\",
    \"@etl_date_key@\":\"@etl_date@\"}).
    forEach(function(x){db.@tmp@.save({\"SQL_ID\":x.SQL_ID,\"PLAN_HASH_VALUE\":x.PLAN_HASH_VALUE})})"""
    sql_collection = mongo_client.get_collection(sql)
    found_items = sql_collection.find({
        "PER_DIRECT_WRITES": {"$gte": direct_writes},
        "USERNMAE": username,
        etl_date_key: etl_date
    })
    for x in found_items:
        yield {
            "SQL_ID": x["SQL_ID"],
            "PLAN_HASH_VALUE": x["PLAN_HASH_VALUE"]
        }


@timing()
def SQL_NO_BIND(mongo_client, sql, username, etl_date_key, etl_date, sql_no_bind_count, **kwargs):
    """db.@sql@.find({\"SUM\":{$gt:@sql_no_bind_count@},\"USERNAME\":\"@username@\",
    \"@etl_date_key@\":\"@etl_date@\"}).
    forEach(function(x){db.@tmp@.save({\"USERNAME\":x.USERNAME,\"SQL_ID\":x.SQL_ID,\"SUM\":x.SUM,
    \"SQL_TEXT_DETAIL\":x.SQL_TEXT_DETAIL,\"SQL_TEXT\":x.SQL_TEXT});})"""
    sql_collection = mongo_client.get_collection(sql)
    found_items = sql_collection.find({
        "SUM": {"$gt": sql_no_bind_count},
        "USERNAME": username,
        etl_date_key: etl_date
    })

    for x in found_items:
        yield {
            "USERNMAE": x["USERNMAE"],
            "SQL_ID": x["SQL_ID"],
            "SUM": x["SUM"],
            "SQL_TEXT_DETAIL": x["SQL_TEXT_DETAIL"],
            "SQL_TEXT": x["SQL_TEXT"]
        }


@timing()
def SQL_TO_CHANGE_TYPE(mongo_client, sql, username, etl_date_key, etl_date, **kwargs):
    """db.@sql@.find({FILTER_PREDICATES:/SYS_OP/,USERNAME:\"@username@\",ETL_DATE:\"@etl_date@\"}).
    forEach(function(x)
    {db.@tmp@.save({SQL_ID:x.SQL_ID,PLAN_HASH_VALUE:x.PLAN_HASH_VALUE,OBJECT_NAME:x.OBJECT_NAME,
    ID:x.ID,COST:x.COST,COUNT:\"\"})})"""
    sql_collection = mongo_client.get_collection(sql)
    found_items = sql_collection.find({
        "FILTER_PREDICATES": re.compile("SYS_OP"),
        "USERNAME": username,
        "ETL_DATE": etl_date
    })
    for x in found_items:
        yield {
            "SQL_ID": x["SQL_ID"],
            "PLAN_HASH_VALUE": x["PLAN_HASH_VALUE"],
            "OBJECT_NAME": x["OBJECT_NAME"],
            "ID": x["ID"],
            "COST": x["COST"],
            "COUNT": ""
        }


def TABLE_ACCESS_BY_GLOBAL_INDEX(mongo_client, sql, username, etl_date_key, etl_date, **kwargs):

    sql_collection = mongo_client.get_collection(sql)
    found_items=sql_collection.find({
        "OPERATION":"TABLE ACCESS",
        "OPTIONS":re.compile(r"BY GLOBAL INDEX ROWID", re.I),
        "USERNAME": username,
        etl_date_key: etl_date
    })
    for x in found_items:
        yield {
            "SQL_ID": x["SQL_ID"],
            "PLAN_HASH_VALUE": x["PLAN_HASH_VALUE"],
            "OBJECT_NAME": x["OBJECT_NAME"],
            "ID": x["ID"],
            "COST": x["COST"],
            "COUNT": ""
        }
