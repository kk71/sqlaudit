def code(rule, **kwargs):
    sql_plan_qs = kwargs["sql_plan_qs"]

    plans = sql_plan_qs.filter(operation="INDEX", options="FULL SCAN")

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
