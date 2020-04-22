def code(rule, entries, **kwargs):
    sql_plan_qs = kwargs["sql_plan_qs"]

    plans = sql_plan_qs.filter(
        operation="PARTITION RANGE",
        options="ITERATOR"
    )
    for x in plans:
        yield {}


code_hole.append(code)
