from models.mongo import ObjTabInfo


def code(rule, **kwargs):
    sql_plan_qs = kwargs["sql_plan_qs"]

    plans = sql_plan_qs.filter(operation="TABLE ACCESS", options="FULL")

    for x in plans:
        tab = ObjTabInfo.objects(table_name=x.object_name,
                                 num_rows__gt=rule.gip("table_row_num")).first()
        if tab:
            return -rule.weight, [
                x.statement_id,
                x.plan_id,
                x.object_name,
                x.the_id,
                x.cost
            ]
    return None, []


code_hole.append(code)
