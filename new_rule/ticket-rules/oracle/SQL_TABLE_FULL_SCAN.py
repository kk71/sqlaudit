from models.mongo import ObjTabInfo


def code(rule, entries, **kwargs):
    '''
    db.@sql@.find({
        OPERATION: 'TABLE ACCESS', OPTIONS: 'FULL', USERNAME: '@username@',
        record_id: '@record_id@'}).forEach(function(x){db.@tmp@.save({
            SQL_ID:x.SQL_ID, PLAN_HASH_VALUE:x.PLAN_HASH_VALUE, OBJECT_NAME:x.OBJECT_NAME,
            ID:x.ID, COST:x.COST, COUNT:''});})"
    '''
    sql_plan_qs = kwargs["sql_plan_qs"]

    plans = sql_plan_qs.filter(operation="TABLE ACCESS", options="FULL")

    for plan in plans:
        return -rule.weight, [
            plan.statement_id,
            plan.plan_id,
            plan.object_name,
            plan.the_id,
            plan.cost
        ]
    return None, []


code_hole.append(code)
