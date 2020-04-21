def code(rule, entries, **kwargs):
    sql_plan_qs = kwargs["sql_plan_qs"]

    plans = sql_plan_qs.filter(operation="PARTITION RANGE", options="ALL")

    for x in plans:
        plans_filter = sql_plan_qs.filter(statement_id=x.statement_id,
                                          plan_id=x.plan_id,
                                          the_id=x.the_id + 1)
        for y in plans_filter:
            return -rule.weight, [
                y.statement_id,
                y.plan_id,
                y.object_name,
                y.the_id,
                y.cost
            ]
    return None, []


code_hole.append(code)
