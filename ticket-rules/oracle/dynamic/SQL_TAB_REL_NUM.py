

def code(rule,**kwargs):
    """db.@sql@.group(
    { key:{\"USERNAME\":1,\"ETL_DATE\":1,\"SQL_ID\":1,\"PLAN_HASH_VALUE\":1,
    \"OBJECT_TYPE\":1},
    cond:{\"USERNAME\":\"@username@\",\"@etl_date_key@\":\"@etl_date@\",
    \"OBJECT_TYPE\":\"TABLE\"},
    reduce:function(curr,result){ result.count++; }, initial:{count:0} } ).
    forEach(function(x)
    {db.@tmp1@.save({\"SQL_ID\":x.SQL_ID,\"PLAN_HASH_VALUE\":x.PLAN_HASH_VALUE,
    \"ID\":x.ID,\"COUNT\":x.count})});
    db.@tmp1@.find({\"COUNT\":{$gte:@tab_num@}}).
    forEach(function(y)
    {db.@tmp@.save({\"SQL_ID\":y.SQL_ID,\"PLAN_HASH_VALUE\":y.PLAN_HASH_VALUE,
    \"ID\":y.ID,\"COUNT\":y.COUNT,\"COST\":\"\",\"OBJECT_NAME\":\"\"})})",
    """
    pass