from models.mongo import ObjIndColInfo


def code(rule, **kwargs):
    sql_plan_qs = kwargs["sql_plan_qs"]

    plans = sql_plan_qs.filter(
        operation="INDEX",
        options="FAST FULL SCAN"
    )

    for x in plans:
        tab = ObjIndColInfo.objects(index_name=x.object_name).first()
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
