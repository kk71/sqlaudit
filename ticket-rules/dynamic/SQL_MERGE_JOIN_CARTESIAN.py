def code(rule, **kwargs):
    sql_plan_qs = kwargs["sql_plan_qs"]

    plans = sql_plan_qs.filter(operation="MERGE JOIN", options="CARTESIAN")

    for x in plans:
        return -rule.weight, [
            x.statement_id,
            x.plan_id,
            x.object_name,
            x.the_id,
            x.cost,
            x.count
        ]
    return None, []


code_hole.append(code)
